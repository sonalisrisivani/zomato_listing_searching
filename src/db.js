const mysql = require('mysql2');
const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const XLSX = require('xlsx');

// Create a MySQL connection
const connection = mysql.createConnection({
  host: 'localhost',
  user: 'root',
  password: 'Sonali@21',
  database: 'zomato'
});

// Connect to the MySQL server
connection.connect(err => {
  if (err) {
    console.error('Error connecting to the database:', err);
    return;
  }
  console.log('Connected to the database.');

  // Ensure the table schema is correct
  ensureTableSchema();
});

function ensureTableSchema() {
  const alterRestaurantsTable = `
    ALTER TABLE restaurants MODIFY currency VARCHAR(100);
  `;

  connection.query(alterRestaurantsTable, (err) => {
    if (err && err.code !== 'ER_BAD_FIELD_ERROR') throw err;
    console.log('Restaurants table schema ensured.');
    createTables();
  });
}

// Function to create tables
function createTables() {
  const createCountriesTable = `
    CREATE TABLE IF NOT EXISTS countries (
      id INT AUTO_INCREMENT PRIMARY KEY,
      name VARCHAR(255) NOT NULL
    );
  `;

  const createLocationsTable = `
    CREATE TABLE IF NOT EXISTS locations (
      id INT AUTO_INCREMENT PRIMARY KEY,
      city VARCHAR(255) NOT NULL,
      country_id INT,
      latitude DECIMAL(10, 7),
      longitude DECIMAL(10, 7),
      address TEXT,
      locality VARCHAR(255),
      locality_verbose VARCHAR(255),
      FOREIGN KEY (country_id) REFERENCES countries(id)
    );
  `;

  const createRestaurantsTable = `
    CREATE TABLE IF NOT EXISTS restaurants (
      id INT AUTO_INCREMENT PRIMARY KEY,
      name VARCHAR(255) NOT NULL,
      location_id INT,
      average_cost_for_two INT,
      price_range INT,
      currency VARCHAR(100),  -- Set length for currency column
      cuisines TEXT,
      aggregate_rating DECIMAL(2, 1),
      rating_text VARCHAR(50),
      votes INT,
      has_online_delivery BOOLEAN,
      has_table_booking BOOLEAN,
      is_delivering_now BOOLEAN,
      deeplink VARCHAR(255),
      menu_url VARCHAR(255),
      photos_url VARCHAR(255),
      featured_image VARCHAR(255),
      thumb VARCHAR(255),
      events_url VARCHAR(255),
      FOREIGN KEY (location_id) REFERENCES locations(id)
    );
  `;

  connection.query(createCountriesTable, (err) => {
    if (err) throw err;
    console.log('Countries table created.');
    loadCountryData();
  });

  connection.query(createLocationsTable, (err) => {
    if (err) throw err;
    console.log('Locations table created.');
  });

  connection.query(createRestaurantsTable, (err) => {
    if (err) throw err;
    console.log('Restaurants table created.');
  });
}







// Function to load country data from the Excel file
function loadCountryData() {
  const workbook = XLSX.readFile(path.join(__dirname, '../data-set/Country-Code.xlsx'));
  const sheetName = workbook.SheetNames[0];
  const countryData = XLSX.utils.sheet_to_json(workbook.Sheets[sheetName]);

  // First, clear the locations table to avoid foreign key constraint issues
  const deleteLocationsQuery = 'DELETE FROM locations';
  connection.query(deleteLocationsQuery, (err) => {
    if (err) throw err;

    const deleteCountriesQuery = 'DELETE FROM countries';
    connection.query(deleteCountriesQuery, (err) => {
      if (err) throw err;

      const insertCountriesQuery = 'INSERT INTO countries (id, name) VALUES ?';
      const values = countryData.map(country => [country['Country Code'], country['Country']]);

      connection.query(insertCountriesQuery, [values], (err) => {
        if (err) throw err;
        console.log('Country data loaded.');
        loadRestaurantData();
      });
    });
  });
}

// Function to load restaurant data from the CSV file
function loadRestaurantData() {
  const filePath = path.join(__dirname, '../data-set/zomato.csv');
  const restaurants = [];

  fs.createReadStream(filePath)
    .pipe(csv())
    .on('data', row => {
      restaurants.push(row);
    })
    .on('end', () => {
      insertRestaurantData(restaurants, 0);
    });
}

// Function to sanitize strings to avoid exceeding column lengths
const sanitizeString = (str, maxLength) => {
  if (str && str.length > maxLength) {
    console.warn(`Trimming data: ${str}`);
    return str.substring(0, maxLength);
  }
  return str;
};

// Function to convert 'Yes'/'No' to 1/0
const convertYesNoToBoolean = (value) => {
  return value.toLowerCase() === 'yes' ? 1 : 0;
};

// Function to insert restaurant data
function insertRestaurantData(restaurants, index) {
  if (index >= restaurants.length) {
    console.log('Restaurant data loaded.');
    connection.end();
    return;
  }

  const row = restaurants[index];
  const locationQuery = `
    INSERT INTO locations (city, country_id, latitude, longitude, address, locality, locality_verbose)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `;

  const restaurantQuery = `
    INSERT INTO restaurants (name, location_id, average_cost_for_two, price_range, currency, cuisines, aggregate_rating, rating_text, votes, has_online_delivery, has_table_booking, is_delivering_now, deeplink, menu_url, photos_url, featured_image, thumb, events_url)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `;

  connection.query(locationQuery, [
    sanitizeString(row['City'], 255),
    row['Country Code'],
    row['Latitude'],
    row['Longitude'],
    sanitizeString(row['Address'], 65535),
    sanitizeString(row['Locality'], 255),
    sanitizeString(row['Locality Verbose'], 255)
  ], (err, result) => {
    if (err) throw err;
    const location_id = result.insertId;

    const sanitizedCurrency = sanitizeString(row['Currency'], 100);
    console.log(`Inserting row with currency: ${sanitizedCurrency} (length: ${sanitizedCurrency.length})`);

    connection.query(restaurantQuery, [
      sanitizeString(row['Restaurant Name'], 255),
      location_id,
      row['Average Cost for two'],
      row['Price range'],
      sanitizedCurrency,  // Ensure currency fits within the column
      sanitizeString(row['Cuisines'], 65535),
      row['Aggregate rating'],
      sanitizeString(row['Rating text'], 50),
      row['Votes'],
      convertYesNoToBoolean(row['Has Online delivery']),
      convertYesNoToBoolean(row['Has Table booking']),
      convertYesNoToBoolean(row['Is delivering now']),
      sanitizeString(row['Deeplink'], 255),
      sanitizeString(row['Menu'], 255),
      sanitizeString(row['Photos'], 255),
      sanitizeString(row['Featured Image'], 255),
      sanitizeString(row['Thumbnail'], 255),
      sanitizeString(row['Events'], 255)
    ], (err) => {
      if (err) {
        console.error(`Error inserting row: ${JSON.stringify(row)}`);
        throw err;
      }
      insertRestaurantData(restaurants, index + 1);
    });
  });
}


