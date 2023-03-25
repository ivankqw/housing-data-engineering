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
            registration_end_date VARCHAR(255),
            estate_agent_license_no VARCHAR(255),
            salesperson_name VARCHAR(255),
            registration_no VARCHAR(255),
            registration_start_date VARCHAR(255),
            estate_agent_name VARCHAR(255),
            _id INT NOT NULL        
        );
        
        CREATE TABLE IF NOT EXISTS salesperson_transactions (
            town VARCHAR(255),
            _id INT NOT NULL,
            district VARCHAR(255),
            salesperson_reg_num VARCHAR(255),
            salesperson_name VARCHAR(255),
            transaction_type VARCHAR(255),
            general_location VARCHAR(255),
            transaction_date VARCHAR(255),
            represented VARCHAR(255),
            property_type VARCHAR(255)
        );
        
        CREATE TABLE IF NOT EXISTS districts (
            postal_district INT,
            postal_sector VARCHAR(255)
        );
        
        CREATE TABLE IF NOT EXISTS private_transactions (
            area VARCHAR(255),
            floor_range VARCHAR(255),
            number_of_units VARCHAR(255),
            contract_date VARCHAR(255),
            type_of_sale VARCHAR(255),
            price VARCHAR(255),
            property_type VARCHAR(255),
            district VARCHAR(255),
            type_of_area VARCHAR(255),
            tenure VARCHAR(255),
            nett_price VARCHAR(255),
            street_name VARCHAR(255),
            project_name VARCHAR(255),
            market_segment VARCHAR(255),
            month VARCHAR(255),
            year VARCHAR(255)
        );
        
        CREATE TABLE IF NOT EXISTS private_rental (
            area_sqm VARCHAR(255),
            lease_date VARCHAR(255),
            property_type VARCHAR(255),
            district VARCHAR(255),
            area_sqft VARCHAR(255),
            number_of_bedrooms VARCHAR(255),
            rental VARCHAR(255),
            street_name VARCHAR(255),
            project_name VARCHAR(255),
            month VARCHAR(255),
            year VARCHAR(255)
        );
                
        CREATE TABLE IF NOT EXISTS hdb_information (
            year_completed VARCHAR(255),
            multigen_sold VARCHAR(255),
            bldg_contract_town VARCHAR(255),
            multistorey_carpark VARCHAR(255),
            street VARCHAR(255),
            total_dwelling_units VARCHAR(255),
            blk_no VARCHAR(255),
            exec_sold VARCHAR(255),
            max_floor_lvl VARCHAR(255),
            residential VARCHAR(255),
            one_room_sold VARCHAR(255),
            precinct_pavilion VARCHAR(255),
            other_room_rental VARCHAR(255),
            five_room_sold VARCHAR(255),
            three_room_sold VARCHAR(255),
            commercial VARCHAR(255),
            four_room_sold VARCHAR(255),
            miscellaneous VARCHAR(255),
            studio_apartment_sold VARCHAR(255),
            two_room_rental VARCHAR(255),
            two_room_sold VARCHAR(255),
            one_room_rental VARCHAR(255),
            three_room_rental VARCHAR(255),
            market_hawker VARCHAR(255),
            _id INT NOT NULL
        );
               
        CREATE TABLE IF NOT EXISTS resale_flats (
            town VARCHAR(255),
            flat_type VARCHAR(255),
            flat_model VARCHAR(255),
            floor_area_sqm VARCHAR(255),
            street_name VARCHAR(255),
            resale_price VARCHAR(255),
            month VARCHAR(255),
            remaining_lease VARCHAR(255),
            lease_commence_date VARCHAR(255),
            storey_range VARCHAR(255),
            id INT NOT NULL,
            block VARCHAR(255),
            year VARCHAR(255),
            street_name_with_block VARCHAR(255),
            postal VARCHAR(255),
            x_coord VARCHAR(255),
            y_coord VARCHAR(255),
            latitude VARCHAR(255),
            longitude VARCHAR(255),
            district VARCHAR(255)
        );
    
        CREATE TABLE IF NOT EXISTS rental_flats (
            town VARCHAR(255),
            flat_type VARCHAR(255),
            monthly_rent VARCHAR(255),
            street_name VARCHAR(255),
            rent_approval_date VARCHAR(255),
            _id INT NOT NULL,
            block VARCHAR(255)
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


