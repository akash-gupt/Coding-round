const express = require('express');
const fetch = require('node-fetch');
const fs = require('fs');
const csv = require('csv-parser');

const {
  MongoClient
} = require('mongodb');


const mongoUri = "mongodb+srv://akash1563:akash1563@timeberline.ivlkc.mongodb.net/test";
const client = new MongoClient(mongoUri);


const app = express();
app.use(express.json())

client.connect((err) => {
  if (err) {
    console.error('connecting to mongodb error');
    process.exit(1);
  }
}).then((data) => {
  console.log('connecting to mongodb successfully')

  const port = process.env.PORT || 3000;
  app.listen(port, () => {
    console.log(`Backend is running ${port}`)
  })
})

app.get('/populate', async (req, res) => {
  try {
    const commentResponse = await fetch("https://jsonplaceholder.typicode.com/comments");
    const comments = await commentResponse.json();

    const csvFilePath = "data.csv"
    const csvResponse = await fetch("http://console.mbwebportal.com/deepak/csvdata.csv");
    const stream = fs.createWriteStream(csvFilePath);
    csvResponse.body.pipe(stream);

    stream.on('finish', async () => {

      const results = [];

      fs.createReadStream(csvFilePath).pipe(csv()).on('data', (data) => {
        results.push(data);
      }).on('end', async () => {

        const db = client.db();
        const collection = db.collection('records');
        const result = await collection.insertMany([...comments, ...results]);
        res.json({
          message: `insertion successful of record count ${result.insertedCount}`
        })
      })

    })

  } catch (error) {
    console.error(error);
    res.status(500).json({
      message: "Error in populating records"
    })
  }

})

app.post('/search', async (req, res) => {
  try {
    const db = client.db();
    const collection = db.collection('records');

    console.log(req.body)

    const {
      name = null, email = null, body = null, limit = null, sort = null
    } = req.body;

    const query = {};

    if (name) query.name = {
      $regex: name,
      $options: 'i'
    };
    if (email) query.email = {
      $regex: email,
      $options: 'i'
    };
    if (body) query.body = {
      $regex: body,
      $options: 'i'
    };

    const cursor = collection.find(query);
    if (limit) cursor.limit(parseInt(limit));
    if (sort) cursor.sort(sort);

    const results = await cursor.toArray();
    res.json({
      results
    })
  } catch (error) {
    console.error(error);
    res.status(500).json({
      message: "Error in search records"
    })
  }
})