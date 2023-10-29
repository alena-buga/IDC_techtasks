# -*- coding: utf-8 -*-
import re
import psycopg2

reSP = re.compile(r'\s{2,}')  # removing 2 and more spaces


def generate_variants(value: str) -> set:
    """Generate SET for matching names"""

    def cleanup_str(text: str) -> str:
        """ Function for deleting 2 and more spase """
        text = reSP.sub(' ', text)
        return text

    def first_name_latter(text: str) -> str:
        """ Transform First Name -> F. Name """
        words = text.split(' ')
        return f'{words[0][0]}. ' + ' '.join(words[1:])

    def last_name_latter(text: str) -> str:
        """ Transform First Name -> First N."""
        words = text.split(' ')
        return ' '.join(words[:-1]) + f' {words[-1][0]}.'

    # Define some the prefixes as a set for faster membership checking
    prefixes = {'mr.', 'ms.', 'mrs.', 'phd.', 'bc.', 'ing.', 'mrg.', 'mba'}

    cleaned_value = cleanup_str(value.lower().replace(',', ' '))  # remove all commas

    # Init SET with original value
    output_set = {cleaned_value}

    # Check prefixes is existing in string
    match_prefix = any(substring in cleaned_value for substring in prefixes)
    if not match_prefix:
        # Abbreviate the name
        v_s = first_name_latter(cleaned_value)
        output_set.add(cleanup_str(v_s.strip()))

    # Reversed string match
    v_swp = ' '.join(reversed(cleaned_value.split(' ')))
    v_swp = cleanup_str(v_swp.strip())
    output_set.add(v_swp)

    # Short match: abbreviate the fist\last name after reverse
    if not match_prefix:
        v_s = first_name_latter(v_swp)
        output_set.add(cleanup_str(v_s.strip()))
    else:
        v_s = last_name_latter(v_swp)
        output_set.add(cleanup_str(v_s.strip()))

    # Remove prefixes from string
    for prefix in prefixes:
        cleaned_value = cleaned_value.replace(prefix, ' ')
    cleaned_value = cleaned_value.strip()
    output_set.add(cleanup_str(cleaned_value))

    # Abbreviate the name after clean prefix
    if '.' not in cleaned_value:
        v_s = first_name_latter(cleaned_value)
        output_set.add(cleanup_str(v_s.strip()))

    # Reverse first & last name
    v_swp = ' '.join(reversed(cleaned_value.split(' ')))
    v_swp = cleanup_str(v_swp.strip())
    output_set.add(v_swp)

    # Abbreviate the name
    v_s = first_name_latter(v_swp)
    output_set.add(cleanup_str(v_s.strip()))

    return output_set


def create_structure_if_not_exists(connection) -> None:
    """Creates tables if it doesn't exist"""
    cursor = connection.cursor()
    query = '''
        CREATE TABLE IF NOT EXISTS table_match (
               name_stat TEXT NOT NULL,
               name_crm TEXT NOT NULL,
               PRIMARY KEY (name_stat, name_crm)
            );
        CREATE TABLE IF NOT EXISTS table_match2 (
               name_stat TEXT NOT NULL,
               name_crm TEXT NOT NULL,
               score integer,
               PRIMARY KEY (name_stat, name_crm)
            );
    '''
    cursor.execute(query)
    connection.commit()


# Processing on DB
# for STAT table
db1 = psycopg2.connect("postgres://alena_buga@localhost:5432/test")
cursor1 = db1.cursor()
cursor1.execute('SELECT distinct name FROM stat')  # get unique NAME
table1 = cursor1.fetchall()

# for CRM table
db2 = psycopg2.connect("postgres://alena_buga@localhost:5432/test")
cursor2 = db2.cursor()
cursor2.execute('SELECT user_name FROM crm')
table2 = cursor2.fetchall()
crm_dict = dict()
for row in table2:
    crm_name = row[0].lower()
    crm_set = generate_variants(crm_name)
    tmp_dict = {x: crm_name for x in crm_set}  # using each variant as key and original name as value
    crm_dict.update(tmp_dict)

create_structure_if_not_exists(db2)

# First decision as a Hash join
# Iterate over table 1
for row in table1:
    name = row[0]
    name_set = generate_variants(name)
    match = name_set & set(crm_dict)  # dictionary keys as SET
    if match:
        # Population table 1 by first matched value
        for m in match:
            sql = cursor2.mogrify("INSERT INTO table_match (name_stat, name_crm) VALUES (%s,%s) "
                                  "ON CONFLICT (name_stat, name_crm) DO NOTHING;", [name.lower(), crm_dict[m]])
            cursor2.execute(sql)
            db2.commit()
            break

# Second decision as a Nested Loop Join
# Matching with external scoring lib. !pip install thefuzz
from thefuzz import fuzz
for row in table1:
    t1_name = row[0]
    for rt2 in table2:
        t2_name = rt2[0]
        score = fuzz.token_sort_ratio(t1_name, t2_name)
        if score >= 70:
            sql = cursor2.mogrify("INSERT INTO table_match2 (name_stat, name_crm, score) VALUES (%s,%s,%s) "
                                  "ON CONFLICT (name_stat, name_crm) DO NOTHING;", [t1_name, t2_name, score])
            cursor2.execute(sql)
            db2.commit()

db1.close()
db2.close()
