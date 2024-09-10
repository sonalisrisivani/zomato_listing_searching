const express = require('express');
const path = require('path');
const mysql = require('mysql');
const app = express();

const db = mysql.createConnection({
  host: 'localhost',
  user: 'root',
  password: 'Sonali@21',
  database: 'zomato'
});




db.connect((err) => {
  if (err) {
    throw err;
  }
  console.log('MySQL connected...');
});



app.use(express.static(path.join(__dirname, 'public')));
app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'src/views'));
 








app.get('/', (req, res) => {
  res.render('index');
});






















// added searching restaruant with id
// here join operation is used, that means if we enter location and restaurent or dish name, it will give reults of common items . if we give only one of those two options, then join of emoplt and self give self.
// added sort functionality if there are restaurents. if sort is not metioned , it will take default as name




app.get('/search', (req, res) => {
  const { q, location, sort } = req.query;
  console.log(`Search query: ${q}, Location query: ${location},  Sort by: ${sort}`); // Log the search and location queries
  console.log('Testing');


  // Check if the search query is a valid restaurant ID
  //10 means decimal, it is triying to change to decimal 

  const id = parseInt(q, 10);
  console.log(`Processed query: ${id}`);
  console.log(`Type of query: ${typeof id}`)

   
  // nan is isnotanum
  if (!isNaN(id)) {
    // Query for restaurant by ID
    const sqlById = `
      SELECT r.*, l.city, l.address, l.locality
      FROM restaurants r
      JOIN locations l ON r.location_id = l.id
      WHERE r.id = ?
    `;
    db.query(sqlById, [id], (err, results) => {
      if (err) {
        return res.status(500).send(err);
      }
      if (results.length > 0) {
        // Redirect to the restaurant detail page
        //return res.json(results[0]);  // since json is an array with single  row 
        return res.redirect(`/restaurant/${id}`);
      } 
      
    });
   return;
  }


  let sql = `
    SELECT r.*
    FROM restaurants r
    JOIN locations l ON r.location_id = l.id
    WHERE (r.name LIKE ? OR r.cuisines LIKE ?)
  `;

  const query = `%${q}%`;
  const params = [query, query];

  if (location) {
    sql += `
      AND (l.city LIKE ? OR l.address LIKE ? OR l.locality LIKE ? OR l.country_id = (SELECT id FROM countries WHERE name LIKE ?))
    `;
    const locationQuery = `%${location}%`;
    params.push(locationQuery, locationQuery, locationQuery, locationQuery);
  }


  // i am giving rating as default
  const sortColumn = sort ? mysql.escapeId(sort) : 'aggregate_rating';
  console.log(`Using sort column: ${sortColumn}`);
  sql += ` ORDER BY ${sortColumn} DESC`;


  console.log(`Executing SQL: ${sql} with params ${params}`);

  db.query(sql, params, (err, results) => {
    if (err) {
      return res.status(500).send(err);
    }
    console.log(`Results found: ${results.length}`); // Log the number of results found
    res.render('index', { results });
    //res.json(results);  // here we are not using results[0] because, each row -one object is stored
  });
});







































//to display theeslected restaurant in details
// to directly display when we add restaurent id
//pagination concept, where we display few restarents in every page


app.get('/restaurant/:id', (req, res) => {

// to dispay restaurent
  const { id } = req.params;

  const sqlRestaurant = `
    SELECT r.*, l.city, l.address, l.locality
    FROM restaurants r
    JOIN locations l ON r.location_id = l.id
    WHERE r.id = ?
  `;

    const sqlPaginated = `
    SELECT r.id, r.name, r.aggregate_rating, r.votes, l.city AS location
    FROM restaurants r
    JOIN locations l ON r.location_id = l.id
    LIMIT 10
  `;

  db.query(sqlRestaurant, [id], (err, restaurantResults) => {
    if (err) {
      return res.status(500).send(err);
    }
    if (restaurantResults.length === 0) {
      return res.status(404).send('Restaurant not found');
    }
    const restaurant = restaurantResults[0];
    

     db.query(sqlPaginated, (err, paginatedResults) => {
      if (err) {
        return res.status(500).send(err);
      }

      res.render('restaurant', { restaurant, paginatedResults });
    });
   });
});














const PORT = 3000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
