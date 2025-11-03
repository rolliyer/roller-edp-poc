import pandas as pd 
import random 
import string 
from io import StringIO 
def generate_random_username(length=8): 
    """Generate a random username""" 
    letters = string.ascii_lowercase 
    return ''.join(random.choice(letters) for i in range(length)) 

def generate_random_phone(): 
    """Generate a random phone number in format XXX-XXX-XXXX""" 
    return f"{random.randint(200, 999)}-{random.randint(200, 999)}-{random.randint(1000, 9999)}" 

def generate_random_email(username): 
    """Generate email from username""" 
    domains = ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'company.com'] 
    return f"{username}@{random.choice(domains)}" 

# Generate 100 user records 
# 
def main(inputs): 
    users = [] 
    for i in range(1, 101): 
        username = generate_random_username() 
        user_data = { 'userid': i, 'username': username, 'email': generate_random_email(username), 'phone_number': generate_random_phone() } 
        users.append(user_data) 
    # Create DataFrame 
    df = pd.DataFrame(users) 
    
    # Convert DataFrame to CSV string 
    csv_buffer = StringIO() 
    df.to_csv(csv_buffer, index=False) 
    csv_content = csv_buffer.getvalue() 
    
    # Return the CSV content and filename for Google Drive 
    result = { "csv_content": csv_content, 
               "filename": f"user_records_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv" 
             } 
    return result