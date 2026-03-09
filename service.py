from sqlalchemy.orm import Session
from models import Contact
from datetime import datetime
from typing import Optional, List


def get_cluster(db: Session, contact_id: int) -> List[Contact]:
    """Get all contacts in the same cluster (linked to same primary)."""
    all_ids = {contact_id}
    changed = True
    
    while changed:
        changed = False
        for cid in list(all_ids):
            # find contacts that link to this one or this one links to
            linked = db.query(Contact).filter(
                ((Contact.linkedId == cid) | (Contact.id == cid)),
                Contact.deletedAt.is_(None)
            ).all()
            for c in linked:
                if c.id not in all_ids:
                    all_ids.add(c.id)
                    changed = True
                if c.linkedId and c.linkedId not in all_ids:
                    all_ids.add(c.linkedId)
                    changed = True
    
    return db.query(Contact).filter(Contact.id.in_(all_ids), Contact.deletedAt.is_(None)).all()


def find_matching_contacts(db: Session, email: Optional[str], phone: Optional[str]) -> List[Contact]:
    """Find all contacts that match email or phone, including linked ones."""
    matches = []
    
    if email:
        matches.extend(db.query(Contact).filter(Contact.email == email, Contact.deletedAt.is_(None)).all())
    
    if phone:
        matches.extend(db.query(Contact).filter(Contact.phoneNumber == phone, Contact.deletedAt.is_(None)).all())
    
    if not matches:
        return []
    
    # get all contacts in clusters of matched contacts
    all_ids = set()
    for match in matches:
        cluster = get_cluster(db, match.id)
        all_ids.update(c.id for c in cluster)
    
    return db.query(Contact).filter(Contact.id.in_(all_ids), Contact.deletedAt.is_(None)).all()


def get_primary_contact(contacts: List[Contact]) -> Contact:
    """Get the oldest primary contact, or oldest contact if no primary exists."""
    primaries = [c for c in contacts if c.linkPrecedence == "primary"]
    if primaries:
        return min(primaries, key=lambda x: x.createdAt)
    return min(contacts, key=lambda x: x.createdAt)


def merge_primaries(db: Session, old_primary: Contact, new_primary: Contact):
    """Demote newer primary to secondary and relink its children."""
    if old_primary.createdAt > new_primary.createdAt:
        old_primary, new_primary = new_primary, old_primary
    
    # demote newer one
    new_primary.linkPrecedence = "secondary"
    new_primary.linkedId = old_primary.id
    new_primary.updatedAt = datetime.utcnow()
    
    # relink children of demoted primary
    children = db.query(Contact).filter(
        Contact.linkedId == new_primary.id,
        Contact.deletedAt.is_(None)
    ).all()
    for child in children:
        child.linkedId = old_primary.id
        child.updatedAt = datetime.utcnow()


def has_info_in_cluster(contacts: List[Contact], email: Optional[str], phone: Optional[str]) -> bool:
    """Check if email or phone already exists in the cluster."""
    cluster_emails = {c.email for c in contacts if c.email}
    cluster_phones = {c.phoneNumber for c in contacts if c.phoneNumber}
    
    if email and email in cluster_emails:
        return True
    if phone and phone in cluster_phones:
        return True
    return False


def identify_contact(db: Session, email: Optional[str], phone: Optional[str]):
    """Main reconciliation logic."""
    if not email and not phone:
        return None
    
    matches = find_matching_contacts(db, email, phone)
    
    if not matches:
        # new primary contact
        contact = Contact(
            email=email,
            phoneNumber=phone,
            linkPrecedence="primary"
        )
        db.add(contact)
        db.commit()
        db.refresh(contact)
        return contact
    
    primary = get_primary_contact(matches)
    
    # check if we need to merge two separate primary clusters
    primaries = [c for c in matches if c.linkPrecedence == "primary"]
    if len(primaries) > 1:
        primaries.sort(key=lambda x: x.createdAt)
        for p in primaries[1:]:
            merge_primaries(db, primaries[0], p)
        db.commit()
        db.refresh(primaries[0])
        primary = primaries[0]
        # refresh matches after merge
        matches = find_matching_contacts(db, email, phone)
    
    # check if we need to create a new secondary with new info
    if has_info_in_cluster(matches, email, phone):
        # all info already in cluster, just return
        pass
    else:
        # create secondary with new info
        new_email = email if email and email not in {c.email for c in matches if c.email} else None
        new_phone = phone if phone and phone not in {c.phoneNumber for c in matches if c.phoneNumber} else None
        
        if new_email or new_phone:
            secondary = Contact(
                email=new_email,
                phoneNumber=new_phone,
                linkedId=primary.id,
                linkPrecedence="secondary"
            )
            db.add(secondary)
            db.commit()
    
    # refresh to get all linked contacts
    db.refresh(primary)
    all_contacts = find_matching_contacts(db, email, phone)
    primary = get_primary_contact(all_contacts)
    
    return primary


def build_response(db: Session, primary: Contact):
    """Build the response structure."""
    contacts = get_cluster(db, primary.id)
    
    emails = set()
    phones = set()
    secondary_ids = []
    
    for c in contacts:
        if c.id == primary.id:
            if c.email:
                emails.add(c.email)
            if c.phoneNumber:
                phones.add(c.phoneNumber)
        else:
            secondary_ids.append(c.id)
            if c.email:
                emails.add(c.email)
            if c.phoneNumber:
                phones.add(c.phoneNumber)
    
    # primary email/phone first
    email_list = []
    phone_list = []
    
    if primary.email:
        email_list.append(primary.email)
    if primary.phoneNumber:
        phone_list.append(primary.phoneNumber)
    
    for email in emails:
        if email != primary.email:
            email_list.append(email)
    
    for phone in phones:
        if phone != primary.phoneNumber:
            phone_list.append(phone)
    
    return {
        "primaryContatctId": primary.id,
        "emails": email_list,
        "phoneNumbers": phone_list,
        "secondaryContactIds": sorted(secondary_ids)
    }

