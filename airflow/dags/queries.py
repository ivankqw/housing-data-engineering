CREATE_TABLES = """
        DROP TABLE IF EXISTS salesperson_information;
        DROP TABLE IF EXISTS salesperson_transactions;
        DROP TABLE IF EXISTS districts;
        DROP TABLE IF EXISTS private_transactions;
        DROP TABLE IF EXISTS private_rental;
        DROP TABLE IF EXISTS hdb_information;
        DROP TABLE IF EXISTS resale_flats;
        DROP TABLE IF EXISTS rental_flats;

        CREATE TABLE IF NOT EXISTS salesperson_information (
            registration_end_date DATE,
            estate_agent_license_no VARCHAR(255),
            salesperson_name VARCHAR(255),
            registration_no VARCHAR(255),
            registration_start_date DATE,
            estate_agent_name VARCHAR(255),
            _id INT NOT NULL
        );
        
        CREATE TABLE IF NOT EXISTS salesperson_transactions (
            town VARCHAR(255),
            _id INT NOT NULL PRIMARY KEY,
            district INT,
            salesperson_reg_num VARCHAR(255),
            salesperson_name VARCHAR(255),
            transaction_type VARCHAR(255),
            general_location VARCHAR(255),
            represented VARCHAR(255),
            property_type VARCHAR(255),
            month VARCHAR(255),
            year VARCHAR(255)
        );
        
        CREATE TABLE IF NOT EXISTS districts (
            postal_district INT NOT NULL PRIMARY KEY,
            postal_sector VARCHAR(255)
        );
        
        CREATE TABLE IF NOT EXISTS private_transactions (
            _id INT NOT NULL PRIMARY KEY,
            area DECIMAL(10,2),
            floor_range VARCHAR(255),
            number_of_units INT,
            contract_date VARCHAR(255),
            type_of_sale INT,
            price INT,
            property_type VARCHAR(255),
            district INT,
            type_of_area VARCHAR(255),
            tenure VARCHAR(255),
            nett_price VARCHAR(255),
            street_name VARCHAR(255),
            project_name VARCHAR(255),
            market_segment VARCHAR(255),
            month INT,
            year INT
        );
        
        CREATE TABLE IF NOT EXISTS private_rental (
            _id INT NOT NULL PRIMARY KEY,
            area_sqm VARCHAR(255),
            lease_date VARCHAR(255),
            property_type VARCHAR(255),
            district INT,
            area_sqft VARCHAR(255),
            number_of_bedrooms DECIMAL(10,2),
            rental INT,
            street_name VARCHAR(255),
            project_name VARCHAR(255),
            month INT,
            year INT
        );
                
        CREATE TABLE IF NOT EXISTS hdb_information (
            year_completed INT,
            multigen_sold INT,
            bldg_contract_town VARCHAR(255),
            multistorey_carpark VARCHAR(255),
            street VARCHAR(255),
            total_dwelling_units INT,
            blk_no VARCHAR(255),
            exec_sold INT,
            max_floor_lvl INT,
            residential VARCHAR(255),
            one_room_sold INT,
            precinct_pavilion VARCHAR(255),
            other_room_rental INT,
            five_room_sold INT,
            three_room_sold INT,
            commercial VARCHAR(255),
            four_room_sold INT,
            miscellaneous VARCHAR(255),
            studio_apartment_sold INT,
            two_room_rental INT,
            two_room_sold INT,
            one_room_rental INT,
            three_room_rental INT,
            market_hawker VARCHAR(255),
            _id INT NOT NULL PRIMARY KEY
        );
               
        CREATE TABLE IF NOT EXISTS resale_flats (
            town VARCHAR(255),
            flat_type VARCHAR(255),
            flat_model VARCHAR(255),
            floor_area_sqm FLOAT,
            street_name VARCHAR(255),
            resale_price FLOAT,
            month INT,
            remaining_lease VARCHAR(255),
            lease_commence_date INT,
            storey_range VARCHAR(255),
            _id INT NOT NULL PRIMARY KEY,
            block VARCHAR(255),
            year INT,
            street_name_with_block VARCHAR(255),
            postal INT,
            x_coord VARCHAR(255),
            y_coord VARCHAR(255),
            latitude VARCHAR(255),
            longitude VARCHAR(255),
            district INT
        );
    
        CREATE TABLE IF NOT EXISTS rental_flats (
            town VARCHAR(255),
            flat_type VARCHAR(255),
            monthly_rent INT,
            street_name VARCHAR(255),
            _id INT NOT NULL PRIMARY KEY,
            block VARCHAR(255),
            year INT,
            month INT,
            street_name_with_block VARCHAR(255),
            postal INT,
            x_coord VARCHAR(255),
            y_coord VARCHAR(255),
            latitude VARCHAR(255),
            longitude VARCHAR(255),
            district INT
        );
    """

INSERT_SALESPERSON_INFORMATION = """
        COPY salesperson_information
        FROM STDIN
        DELIMITER ','
        CSV HEADER;
    """

INSERT_SALESPERSON_TRANSACTIONS = """
        COPY salesperson_transactions
        FROM STDIN
        DELIMITER ','
        CSV HEADER;
    """

INSERT_DISTRICTS = """
        COPY districts
        FROM STDIN
        DELIMITER ','
        CSV HEADER;
    """

INSERT_PRIVATE_TRANSACTIONS = """
        COPY private_transactions
        FROM STDIN
        DELIMITER ','
        CSV HEADER;
    """

INSERT_PRIVATE_RENTAL = """
        COPY private_rental
        FROM STDIN
        DELIMITER ','
        CSV HEADER;
    """

INSERT_HDB_INFORMATION = """
        COPY hdb_information
        FROM STDIN
        DELIMITER ','
        CSV HEADER;
    """

INSERT_RESALE_FLATS = """
        COPY resale_flats 
        FROM STDIN
        DELIMITER ','
        CSV HEADER;
    """

INSERT_RENTAL_FLATS = """
        COPY rental_flats
        FROM STDIN
        DELIMITER ','
        CSV HEADER;
    """

ALTER_TABLES = """
    ALTER TABLE districts ADD CONSTRAINT unique_postal_district UNIQUE (postal_district);            
    ALTER TABLE salesperson_information ADD CONSTRAINT unique_registration_no UNIQUE (registration_no);
    ALTER TABLE salesperson_transactions ADD CONSTRAINT fk_salesperson_registration_no FOREIGN KEY (salesperson_reg_num) REFERENCES salesperson_information (registration_no);
    ALTER TABLE salesperson_transactions ADD CONSTRAINT fk_salesperson_district FOREIGN KEY (district) REFERENCES districts (postal_district);
    ALTER TABLE private_transactions ADD CONSTRAINT fk_private_district FOREIGN KEY (district) REFERENCES districts (postal_district);
    ALTER TABLE private_rental ADD CONSTRAINT fk_private_rental_district FOREIGN KEY (district) REFERENCES districts (postal_district);
    ALTER TABLE resale_flats ADD CONSTRAINT fk_resale_district FOREIGN KEY (district) REFERENCES districts (postal_district);
    ALTER TABLE rental_flats ADD CONSTRAINT fk_rental_district FOREIGN KEY (district) REFERENCES districts (postal_district);
"""
