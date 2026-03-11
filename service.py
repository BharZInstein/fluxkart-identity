from sqlalchemy.orm import Session
from models import Contact
from datetime import datetime
from typing import Optional, List


def get_cluster(db: Session, contact_id: int) -> List[Contact]:
    all_ids = {contact_id}
    changed = True
    
    while changed:
        changed = False
        for cid in list(all_ids):
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
    matches = []
    
    if email:
        matches.extend(db.query(Contact).filter(Contact.email == email, Contact.deletedAt.is_(None)).all())
    
    if phone:
        matches.extend(db.query(Contact).filter(Contact.phoneNumber == phone, Contact.deletedAt.is_(None)).all())
    
    if not matches:
        return []
    
    all_ids = set()
    for match in matches:
        cluster = get_cluster(db, match.id)
        all_ids.update(c.id for c in cluster)
    
    return db.query(Contact).filter(Contact.id.in_(all_ids), Contact.deletedAt.is_(None)).all()


def get_primary_contact(contacts: List[Contact]) -> Contact:
    primaries = [c for c in contacts if c.linkPrecedence == "primary"]
    if primaries:
        return min(primaries, key=lambda x: x.createdAt)
    return min(contacts, key=lambda x: x.createdAt)


def merge_primaries(db: Session, old_primary: Contact, new_primary: Contact):
    if old_primary.createdAt > new_primary.createdAt:
        old_primary, new_primary = new_primary, old_primary
    
    new_primary.linkPrecedence = "secondary"
    new_primary.linkedId = old_primary.id
    new_primary.updatedAt = datetime.utcnow()
    
    children = db.query(Contact).filter(
        Contact.linkedId == new_primary.id,
        Contact.deletedAt.is_(None)
    ).all()
    for child in children:
        child.linkedId = old_primary.id
        child.updatedAt = datetime.utcnow()


def has_info_in_cluster(contacts: List[Contact], email: Optional[str], phone: Optional[str]) -> bool:
    cluster_emails = {c.email for c in contacts if c.email}
    cluster_phones = {c.phoneNumber for c in contacts if c.phoneNumber}
    
    email_known = (not email) or (email in cluster_emails)
    phone_known = (not phone) or (phone in cluster_phones)
    
    return email_known and phone_known


def identify_contact(db: Session, email: Optional[str], phone: Optional[str]):
    if not email and not phone:
        return None
    
    matches = find_matching_contacts(db, email, phone)
    print(f"\n--- identify called: email={email}, phone={phone} ---")
    print(f"matches found: {[(c.id, c.email, c.phoneNumber, c.linkPrecedence) for c in matches]}")

    if not matches:
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
    print(f"primary: {primary.id}")

    primaries = [c for c in matches if c.linkPrecedence == "primary"]
    if len(primaries) > 1:
        primaries.sort(key=lambda x: x.createdAt)
        for p in primaries[1:]:
            merge_primaries(db, primaries[0], p)
        db.commit()
        db.refresh(primaries[0])
        primary = primaries[0]
        matches = find_matching_contacts(db, email, phone)
    
    already_known = has_info_in_cluster(matches, email, phone)
    print(f"already known: {already_known}")

    if not already_known:
        new_email = email if email and email not in {c.email for c in matches if c.email} else None
        new_phone = phone if phone and phone not in {c.phoneNumber for c in matches if c.phoneNumber} else None
        print(f"creating secondary: email={new_email}, phone={new_phone}")
        
        if new_email or new_phone:
            secondary = Contact(
                email=new_email,
                phoneNumber=new_phone,
                linkedId=primary.id,
                linkPrecedence="secondary"
            )
            db.add(secondary)
            db.commit()
    
    db.refresh(primary)
    all_contacts = find_matching_contacts(db, email, phone)
    primary = get_primary_contact(all_contacts)
    
    return primary


def build_response(db: Session, primary: Contact):
    contacts = get_cluster(db, primary.id)
    
    email_list = []
    phone_list = []
    secondary_ids = []
    
    if primary.email:
        email_list.append(primary.email)
    if primary.phoneNumber:
        phone_list.append(primary.phoneNumber)
    
    for c in contacts:
        if c.id == primary.id:
            continue
        secondary_ids.append(c.id)
        if c.email and c.email not in email_list:
            email_list.append(c.email)
        if c.phoneNumber and c.phoneNumber not in phone_list:
            phone_list.append(c.phoneNumber)
    
    return {
        "primaryContatctId": primary.id,
        "emails": email_list,
        "phoneNumbers": phone_list,
        "secondaryContactIds": sorted(secondary_ids)
 
    }
