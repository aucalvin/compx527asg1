// server.js

/**
 * Required External Modules
 */
const express = require("express");
const path = require("path");
const aws = require("aws-sdk");
const athena = require("athena-express");
const plotly = require("chart.js");

/**
 * App Variables
 */
const app = express();
const port = process.env.PORT || "8081";
/**
 *  App Configuration
 */
app.set("views", path.join(__dirname, "views"));
app.set("view engine", "pug");
app.set(port);
app.use(express.static(path.join(__dirname, 'public')));
/**
 * AWS Athena access
 */
let credsString = "[default]\n" +
    "aws_access_key_id=ASIAWUDQXE65IE7BV47C\n" +
    "aws_secret_access_key=t1CqAohuThqVsw5slrsnCND9xMLenDEp/UrrMVYI\n" +
    "aws_session_token=FwoGZXIvYXdzENX//////////wEaDDr7FO1JjpYHtjf2ACLLAQPw7iXxxnYvBSkFrtxYosyyQyEq7lBEyRy1ZB6h2pCuCUx+OklYmkrfaOa+GXV2V7g9yZyEmQD8wBt49w7hNvWnRY8uhrmg6IpQtbDVKdz/oL2LU1/yXYgFH5rmoASmcDc2fb51JQCD5CGPuvm0HlIhDEEJEfvShknquS/E/mGT1t5Ggyt3CGhdATU7kSs52jIpnmZITjn0LaxWXkTR05jo2aRtD9TanulTy4wRtYHZZkFfmnmUAcUkh5mlJyrSe/bbjVfWPmtGLt6OKIrtgPsFMi3K4A6c3qWgsARuFl7yzd+2YQR17Dlq0mUg6Y8guXgEv7Q/g8vIrg80iQoSMX4=";

const awsCredentials = readCredsFromString(credsString);


console.log(awsCredentials);

// const awsCredentials = {
//     region: "us-east-1",
//     accessKeyId: "ASIAZGX6W5QIKUMC2NM5",
//     secretAccessKey: "5nfd/WA+A6ZQfUdipmpT6DmdFuvhf/uOh5nGzMSA",
//     sessionToken: "FwoGZXIvYXdzELb//////////wEaDNjB/wryyi/IxK/hwSLKAaiSbI5aPcc7DtdPloCV9hkGhn55f8CAG0+1JZRChaXKPAeUUtoS6Db+bXJCc3rj9wNDitQD8SHv5XjZUgewdcoZVnnOb/nP2wZADFT7CFw/AiObNeFI553J7W9f4Tl9yzA4uNDFe89DWNKj3uNZsDK6Nim5Mm3j2MF6qYFoXqE6l+wMe/PKtXN9S3nwNRfD397aFYGeQ1sBsb8K1523l5GnWPzvkCoWRgf64rhOeNU5jNlFFTcc47sgxoHmorTGSc9xplnk6A60ZX0o0OTB+gUyLZ3G9YMsF9gmUcpcPAbGU5VVIeDZ+cgnUdeziRWsFHRkJshoHRSnumESU1UboA=="
// };

aws.config.update(awsCredentials);

const athenaExpressConfig = {
    aws,
    s3: "s3://calvinaycompx527asg1",
    getStats: true
};
const athenaExpress = new athena.AthenaExpress(athenaExpressConfig);

const assignmentTitle = "Amazing COMPX527 Assignment Application";
/**
 * Routes Definitions
 */
app.get("/comparison", (req, res) => {
    //Invoking a query on Amazon Athena
    (async () => {
        let bookSalesQuery = {
            sql: "SELECT year, product_category, COUNT(review_id) AS reviews \n" +
                "FROM amazon_reviews_parquet\n" +
                "WHERE product_category = 'Books'\n" +
                "GROUP BY product_category, year\n" +
                "ORDER BY year ASC\n" +
                "\n",
            db: "mydatabase"
        };
        let ebookSalesQuery = {
            sql: "SELECT year, product_category, COUNT(review_id) AS reviews \n" +
                "FROM amazon_reviews_parquet\n" +
                "WHERE product_category = 'Digital_Ebook_Purchase'\n" +
                "GROUP BY product_category, year\n" +
                "ORDER BY year ASC",
            db: "mydatabase"
        };

        try {
            let bookQueryResults = (await athenaExpress.query(bookSalesQuery));
            let ebookQueryResults = (await athenaExpress.query(ebookSalesQuery));


            let bookData = bookQueryResults.Items;
            let ebookData = ebookQueryResults.Items;

            let customers = [];
            let bookReviews = [];
            let ebookReviews = [];
            let years = [];

            for (let i = 0; i < bookData.length; i++) {
                bookReviews.push(bookData[i].reviews);
                years.push(bookData[i].year)
            }

            for (let i = 0; i < ebookData.length; i++) {
                ebookReviews.push(ebookData[i].reviews);
            }

            //add zeroes to account for missing years
            for (let i = 0; i < bookQueryResults.Count - ebookQueryResults.Count; i++) {
                ebookReviews.unshift(0);
            }

            res.render("comparison", { title: assignmentTitle,heading:"Number of Reviews for Physical Books and Ebooks", book_reviews:JSON.stringify(bookReviews),ebook_reviews:JSON.stringify(ebookReviews), years:JSON.stringify(years)});


        } catch (error) {
            console.log(error);
        }
    })();

});

app.get("/products", (req, res) => {

    console.log("made it!");
    (async ()=>{
        let query = {
            sql: "SELECT product_category, COUNT(review_id) AS reviews \n" +
                "FROM amazon_reviews_parquet \n" +
                "WHERE product_category NOT IN ('Books','Digital_Ebook_Purchase') \n" +
                "GROUP BY product_category ORDER BY reviews DESC",
            db: "mydatabase"
        };

        try{
            let queryResults = await athenaExpress.query(query);
            // console.log(queryResults);
            let graphData = {category:[],reviews:[]};

            let data = queryResults.Items;
            for (let i = 0; i < data.length; i++) {
                graphData.category.push(data[i].product_category);
                graphData.reviews.push(data[i].reviews);
            }
            console.log(graphData);

            res.render("products", {title:assignmentTitle, data:JSON.stringify(graphData)});
        }catch (error) {
            console.log(error);
        }
    })();
});

app.get("/customers", (req, res) => {

    console.log("made it!");
    (async ()=>{
        let customersQuery = {
            sql: "SELECT marketplace, year, COUNT(DISTINCT customer_id) \n" +
                "as customers FROM amazon_reviews_parquet \n" +
                "GROUP BY marketplace,year \n" +
                "ORDER BY marketplace, year ASC",
            db: "mydatabase"
        };

        try{
            let customersResults = await athenaExpress.query(customersQuery);
            let data = customersResults.Items;
            let graphData = {DE:[],FR:[],JP:[],UK:[],US:[]}
            let years = [];

            for (let i = 0; i < data.length; i++) {
                next = data[i];
                customers = next.customers;
                switch (next.marketplace) {
                    case 'DE':
                        graphData.DE.push(customers);
                        break;
                    case 'FR':
                        graphData.FR.push(customers);
                        break;
                    case 'JP':
                        graphData.JP.push(customers);
                        break;
                    case 'UK':
                        graphData.UK.push(customers);
                        break;
                    case 'US':
                        graphData.US.push(customers);
                        years.push(next.year);
                        break;
                }
            }

            //pad datasets to be same length
            let desired_length = years.length;
            console.log("years: "+years.length);
            for(let field in graphData){
                let dataset = graphData[field];
                console.log(field);
                // console.log(dataset.length);
                console.log(desired_length-graphData[field].length);
                let initial_length = graphData[field].length;
                for (let i = 0; i < desired_length-initial_length; i++) {
                    graphData[field].unshift(0);
                }
                // console.log(graphData[field]);
            }
            // console.log(graphData);

            res.render("customers", {title:assignmentTitle,graphTitle:"Customer growth excluding US",years:JSON.stringify(years), data:JSON.stringify(graphData)});
        }catch (error) {
            console.log(error);
        }
    })();
});

app.get("/", (req, res) => {
    res.render("home", {title:assignmentTitle});
});

/**
 * Server Activation
 */
app.listen(port, () => {
    console.log(`Listening to requests on http://localhost:${port}`);
});

/*
functions
 */

function readCredsFromString(credsString) {

    let credsSplit = credsString.split(/\r?\n/);
    let access_key_id = extractKey(credsSplit[1]);
    let secret_key = extractKey(credsSplit[2]);
    let session_token = extractKey(credsSplit[3]);

    return {
        region: "us-east-1",
        accessKeyId: access_key_id,
        secretAccessKey: secret_key,
        sessionToken: session_token
    };

}

function extractKey(line){
    let start = line.indexOf('=')+1;
    let end = line.length;
    return line.substring(start, end);
}