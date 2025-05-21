import random
import psycopg2
from faker import Faker
from datetime import timedelta
from tqdm import tqdm

# Initialize Faker
fake = Faker()

# Connect to PostgreSQL (update these values)
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="Library",
    user="abdurrehman",
    password="library.cs"
)

cursor = conn.cursor()

def insert_members(n=200000):
    for _ in tqdm(range(n), desc="Inserting members Please wait....!"):
        name = fake.name()
        email = fake.email()
        phone = fake.phone_number()
        address = fake.address().replace('\n', ', ')
        join_date = fake.date_between(start_date='-3y', end_date='today')
        is_active = random.choice([True, False])
        try:
            cursor.execute("""
                INSERT INTO members (name, email, phone, address, join_date, is_active)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (name, email, phone, address, join_date, is_active))
        except psycopg2.errors.UniqueViolation:
            conn.rollback()

def insert_books(n=500000):
    genres = ['Fiction', 'Non-Fiction', 'Computer-science','Database','Fantasy', 'Biography', 'Mystery', 'Science', 'History', 'Romance']
    publishers = ['Penguin','Khan press', 'Rehman company', 'HarperCollins', 'Random House', 'Oxford Press', 'Cambridge Press']

    for _ in tqdm(range(n), desc="Inserting books Please wait....!"):
        isbn = fake.isbn13()
        title = fake.sentence(nb_words=4).rstrip('.')
        author = fake.name()
        publisher = random.choice(publishers)
        year = random.randint(1950, 2024)
        genre = random.choice(genres)
        total = random.randint(1, 20)
        available = random.randint(0, total)
        try:
            cursor.execute("""
                INSERT INTO books (isbn, title, author, publisher, year_published, genre, total_copies, available_copies)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (isbn, title, author, publisher, year, genre, total, available))
        except psycopg2.errors.UniqueViolation:
            conn.rollback()

def insert_staff(n=500):
    roles = ['Librarian', 'Assistant', 'Manager', 'Technician', 'Intern', 'Security', 'Cleaner', 'Supervisor', 'Administrator'] 
    for _ in tqdm(range(n), desc="Inserting staff Please wait....!"):
        name = fake.name()
        email = fake.email()
        role = random.choice(roles)
        joined = fake.date_between(start_date='-5y', end_date='today')
        try:
            cursor.execute("""
                INSERT INTO staff (name, email, role, joined_date)
                VALUES (%s, %s, %s, %s)
            """, (name, email, role, joined))
        except psycopg2.errors.UniqueViolation:
            conn.rollback()

def insert_borrow_transactions(n=50000):
    cursor.execute("SELECT member_id FROM members")
    member_ids = [row[0] for row in cursor.fetchall()]

    cursor.execute("SELECT book_id FROM books WHERE available_copies > 0")
    book_ids = [row[0] for row in cursor.fetchall()]

    for _ in tqdm(range(n), desc="Inserting borrow transactions Please wait....!"):
        if not book_ids:
            print("No available books to borrow.")
            break
        member_id = random.choice(member_ids)
        book_id = random.choice(book_ids)
        borrow_date = fake.date_between(start_date='-2y', end_date='-1d')
        due_date = borrow_date + timedelta(days=random.randint(7, 30))

        if random.random() < 0.7:
            return_date = fake.date_between(start_date=borrow_date, end_date=due_date + timedelta(days=10))
            fine = max(0, (return_date - due_date).days) * 1.0
        else:
            return_date = None
            fine = 0.0

        cursor.execute("""
            INSERT INTO borrow_transactions (member_id, book_id, borrow_date, due_date, return_date, fine)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (member_id, book_id, borrow_date, due_date, return_date, fine))

def insert_book_reservations(n=50000):
    cursor.execute("SELECT member_id FROM members")
    member_ids = [row[0] for row in cursor.fetchall()]

    cursor.execute("SELECT book_id FROM books")
    book_ids = [row[0] for row in cursor.fetchall()]

    statuses = ['pending', 'fulfilled', 'cancelled']

    for _ in tqdm(range(n), desc="Inserting book reservations Please wait....!"):
        if not book_ids:
            print("No available books to reserve.")
            break
        member_id = random.choice(member_ids)
        book_id = random.choice(book_ids)
        reservation_date = fake.date_between(start_date='-2y', end_date='today')
        status = random.choices(statuses, weights=[0.6, 0.3, 0.1])[0]

        cursor.execute("""
            INSERT INTO book_reservations (book_id, member_id, reservation_date, status)
            VALUES (%s, %s, %s, %s)
        """, (book_id, member_id, reservation_date, status))

#main function to populate the database
def populate():
    insert_members()
    #insert_books()
    #insert_staff()
    conn.commit()  # Commit before FK-dependent inserts
    #insert_borrow_transactions()
    #insert_book_reservations()
    conn.commit()
    print("✅ Congratulations! Data population completed.")

if __name__ == "__main__":
    try:
        populate()
    except Exception as e:
        print("❌ Error:", e)
        conn.rollback()
    finally:
        cursor.close()
        conn.close()
