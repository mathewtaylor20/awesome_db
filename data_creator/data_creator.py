import sys
import csv
import fileinput



def main(args=None):
    addresses = ['Tressillian Road', 'Penny Lane',    'Forest Hill',    'Orchard Close', 'Oak Drive',
                 'Ceader Drive',     'Lewisham Vale', 'Mangrove Swamp', 'Ash Park',      'Rose Lane']

    towns = ['London', 'Liverpool', 'York', 'Leeds', 'Manchester', 'Hove', 'Brighton', 'Poole', 'Guildford', 'Anywhere']







    user_first_name = ['John',  'Mary',   'Ian',  'Tom',     'Tim',  'Steve', 'Andy',  'Ben', 'James', 'Peter',
                  'Wendy', 'Angela', 'Tony', 'Medrith', 'Emma', 'Stacy', 'Tammy', 'Lucy', 'Oliver', 'Heather']

    user_last_name = ['Taylor', 'Smith', 'Naveed', 'Milne', 'Jones', 'Archer', 'Fry', 'Paxman', 'Haart', 'Webber']

    with open('../data_created/address.csv', 'wb') as csvfile:
        table_Writer = csv.writer(csvfile,
                                  delimiter=',',
                                  quotechar='|',
                                  quoting=csv.QUOTE_MINIMAL)

        i = 1
        for address in addresses:
            for town in towns:
                j = 1
                while j < 101:
                    row = [i, j, address, town]
                    table_Writer.writerow(row)
                    i = i + 1
                    j = j + 1
    with open('../data_created/address', 'wb') as binaryfile:
        table_Writer = csv.writer(csvfile,
                                  delimiter=',',
                                  quotechar='|',
                                  quoting=csv.QUOTE_MINIMAL)

        i = 1
        for address in addresses:
            for town in towns:
                j = 1
                while j < 101:
                    row = [i, j, address, town]
                    table_Writer.writerow(row)
                    i = i + 1
                    j = j + 1



    with open('../data_created/user.csv', 'wb') as csvfile:
        table_Writer = csv.writer(csvfile,
                                  delimiter=',',
                                  quotechar='|',
                                  quoting=csv.QUOTE_MINIMAL)

        i = 1
        age = 20
        address = 0
        count = 0
        while count < 50:
            for first_name in user_first_name:
                for last_name in user_last_name:
                    if age == 71:
                        age = 20
                    if address == 4500:
                        address = 1
                    row = [i, first_name, last_name, age, address]
                    table_Writer.writerow(row)
                    i = i + 1
                    age = age + 1
                    address = address + 1
            count = count + 1










if __name__ == "__main__":
    main()