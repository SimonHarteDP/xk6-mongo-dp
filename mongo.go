package xk6_mongo_dp

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"os"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	k6modules "go.k6.io/k6/js/modules"
)

// Register the extension on module initialization, available to
// import from JS as "k6/x/mongo-dp".
func init() {
	k6modules.Register("k6/x/mongo-dp", new(Mongo))
}

// Mongo is the k6 extension for a Mongo client.
type Mongo struct{}

// Client is the Mongo client wrapper.
type Client struct {
	client *mongo.Client
}

type UpsertOneModel struct {
	Query  any `json:"query"`
	Update any `json:"update"`
}

type Response struct {
	Err    error
	Result any
}

type MongoConfig struct {
	Username    string `json:"username"`
	Password    string `json:"password"`
	URL         string `json:"url"`
	Certificate string `json:"certificate"`
	UseTLS      bool   `json:"use_tls"`
}

// NewClient represents the Client constructor (i.e. `new mongo.Client()`) and
// returns a new Mongo client object.
// connURI -> mongodb://username:password@address:port/db?connect=direct
func (m *Mongo) NewClient(config MongoConfig) *Client { return m.NewClientWithOptions(config) }

func (*Mongo) NewClientWithOptions(cfg MongoConfig) *Client {
	// Build the MongoDB connection URI
	uri := fmt.Sprintf("mongodb://%s:%s@%s", cfg.Username, cfg.Password, cfg.URL)
	clientOptions := &options.ClientOptions{}

	// Load the certificate
	tlsConfig := &tls.Config{}
	if cfg.UseTLS {
		caCert, err := os.ReadFile(cfg.Certificate)
		if err != nil {
			log.Fatalf("failed to read certificate file: %v", err)
			return nil
		}

		// Create a certificate pool
		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			log.Fatalf("failed to append certificate to pool: %v", err)
			return nil
		}

		// Configure TLS
		tlsConfig.RootCAs = caCertPool

		// Set client options
		clientOptions = options.Client().ApplyURI(uri).SetTLSConfig(tlsConfig)
	} else {
		clientOptions = options.Client().ApplyURI(uri)
	}

	// Create the MongoDB client
	client, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		log.Fatalf("failed to connect to MongoDB: %v", err)
		return nil
	}

	return &Client{client: client}
}

func (c *Client) Insert(database string, collection string, doc interface{}) Response {
	db := c.client.Database(database)
	col := db.Collection(collection)
	_, err := col.InsertOne(context.Background(), doc)
	if err != nil {
		log.Printf("Error while inserting document: %v", err)
		return Response{Err: err}
	}
	log.Print("Document inserted successfully")
	return Response{Result: doc}
}

func (c *Client) InsertMany(database string, collection string, docs []interface{}) Response {
	db := c.client.Database(database)
	col := db.Collection(collection)
	_, err := col.InsertMany(context.Background(), docs)
	if err != nil {
		log.Printf("Error while inserting multiple documents: %v", err)
		return Response{Err: err}
	}
	return Response{Result: col}
}

func (c *Client) Upsert(database string, collection string, filter interface{}, upsert interface{}) Response {
	db := c.client.Database(database)
	col := db.Collection(collection)
	opts := options.Update().SetUpsert(true)
	_, err := col.UpdateOne(context.Background(), filter, upsert, opts)
	if err != nil {
		log.Printf("Error while performing upsert: %v", err)
		return Response{Err: err}
	}
	return Response{Result: upsert}
}

func (c *Client) Find(database string, collection string, filter interface{}, sort interface{}, limit int64) Response {
	db := c.client.Database(database)
	col := db.Collection(collection)
	opts := options.Find().SetSort(sort).SetLimit(limit)
	cur, err := col.Find(context.Background(), filter, opts)
	if err != nil {
		log.Printf("Error while finding documents: %v", err)
		return Response{Err: err}
	}
	var results []bson.M
	if err = cur.All(context.Background(), &results); err != nil {
		log.Printf("Error while decoding documents: %v", err)
		return Response{Err: err}
	}
	return Response{Result: results}
}

func (c *Client) Aggregate(database string, collection string, pipeline interface{}) Response {
	db := c.client.Database(database)
	col := db.Collection(collection)
	cur, err := col.Aggregate(context.Background(), pipeline)
	if err != nil {
		log.Printf("Error while aggregating: %v", err)
		return Response{Err: err}
	}
	var results []bson.M
	if err = cur.All(context.Background(), &results); err != nil {
		log.Printf("Error while decoding documents: %v", err)
		return Response{Err: err}
	}
	return Response{Result: results}
}

func (c *Client) FindOne(database string, collection string, filter map[string]string) Response {
	db := c.client.Database(database)
	col := db.Collection(collection)
	var result bson.M
	err := col.FindOne(context.Background(), filter).Decode(&result)
	if err != nil {
		return Response{Err: err}
	}

	return Response{Result: result}
}

func (c *Client) UpdateOne(database string, collection string, filter interface{}, data bson.D) Response {
	db := c.client.Database(database)
	col := db.Collection(collection)

	_, err := col.UpdateOne(context.Background(), filter, data)
	if err != nil {
		log.Printf("Error while updating the document: %v", err)
		return Response{Err: err}
	}

	return Response{}
}

func (c *Client) UpdateMany(database string, collection string, filter interface{}, data bson.D) Response {
	db := c.client.Database(database)
	col := db.Collection(collection)

	update := bson.D{{"$set", data}}

	_, err := col.UpdateMany(context.Background(), filter, update)
	if err != nil {
		log.Printf("Error while updating the documents: %v", err)
		return Response{Err: err}
	}

	return Response{}
}

func (c *Client) FindAll(database string, collection string) Response {
	db := c.client.Database(database)
	col := db.Collection(collection)
	cur, err := col.Find(context.Background(), bson.D{{}})
	if err != nil {
		log.Printf("Error while finding documents: %v", err)
		return Response{Err: err}
	}

	var results []bson.M
	if err = cur.All(context.Background(), &results); err != nil {
		log.Printf("Error while decoding documents: %v", err)
		return Response{Err: err}
	}

	return Response{Result: results}
}

func (c *Client) DeleteOne(database string, collection string, filter map[string]string) Response {
	db := c.client.Database(database)
	col := db.Collection(collection)
	_, err := col.DeleteOne(context.Background(), filter)
	if err != nil {
		log.Printf("Error while deleting the document: %v", err)
		return Response{Err: err}
	}

	return Response{}
}

func (c *Client) DeleteMany(database string, collection string, filter map[string]string) Response {
	db := c.client.Database(database)
	col := db.Collection(collection)
	_, err := col.DeleteMany(context.Background(), filter)
	if err != nil {
		log.Printf("Error while deleting the documents: %v", err)
		return Response{Err: err}
	}

	return Response{}
}

func (c *Client) Distinct(database string, collection string, field string, filter interface{}) Response {
	db := c.client.Database(database)
	col := db.Collection(collection)
	result, err := col.Distinct(context.Background(), field, filter)
	if err != nil {
		log.Printf("Error while getting distinct values: %v", err)
		return Response{Err: err}
	}

	return Response{Result: result}
}

func (c *Client) DropCollection(database string, collection string) Response {
	db := c.client.Database(database)
	col := db.Collection(collection)
	err := col.Drop(context.Background())
	if err != nil {
		log.Printf("Error while dropping the collection: %v", err)
		return Response{Err: err}
	}

	return Response{}
}

func (c *Client) CountDocuments(database string, collection string, filter interface{}) Response {
	db := c.client.Database(database)
	col := db.Collection(collection)
	count, err := col.CountDocuments(context.Background(), filter)
	if err != nil {
		log.Printf("Error while counting documents: %v", err)
		return Response{Err: err, Result: 0}
	}
	return Response{Result: count}
}

func (c *Client) FindOneAndUpdate(database string, collection string, filter interface{}, update interface{}) Response {
	db := c.client.Database(database)
	col := db.Collection(collection)
	opts := options.FindOneAndUpdate().SetReturnDocument(options.After)
	result := col.FindOneAndUpdate(context.Background(), filter, update, opts)
	if result.Err() != nil {
		log.Printf("Error while finding and updating document: %v", result.Err())
		return Response{Err: result.Err()}
	}
	return Response{Result: result}
}

func (c *Client) Disconnect() error {
	err := c.client.Disconnect(context.Background())
	if err != nil {
		log.Printf("Error while disconnecting from the database: %v", err)
		return err
	}

	return nil
}
