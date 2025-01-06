package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"time"

	"fmt"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Your MongoDB Atlas Connection String
const uri = "mongodb://127.0.0.1/?retryWrites=true&w=majority"

// A global variable that will hold a reference to the MongoDB client
var mongoClient *mongo.Client
var redisClient *redis.Client

// The init function will run before our main function to establish a connection to MongoDB. If it cannot connect it will fail and the program will exit.
func init() {
	if err := connect_to_mongodb(); err != nil {
		log.Fatal("Could not connect to MongoDB")
	}

	if err := connect_to_redis(); err != nil {
		log.Fatal("Could not connect to Redis")
	}
}
func main() {
	r := gin.Default()
	r.GET("/", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"message": "Hello World",
		})
	})
	r.GET("/jobs/:id", getJobByID)
	r.GET("/jobs/skills", getAllSkills)
	r.GET("/jobs/countries", getAllCountries)
	r.Run()
}

// Our implementation logic for connecting to MongoDB
func connect_to_mongodb() error {
	serverAPI := options.ServerAPI(options.ServerAPIVersion1)
	opts := options.Client().ApplyURI(uri).SetServerAPIOptions(serverAPI).SetCompressors([]string{"zstd"})

	client, err := mongo.Connect(context.TODO(), opts)
	if err != nil {
		panic(err)
	}
	err = client.Ping(context.TODO(), nil)
	mongoClient = client
	return err
}

func connect_to_redis() error {
	redisClient = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
		Protocol: 2,
	})
	_, err := redisClient.Ping(context.Background()).Result()
	return err
}

func getJobByID(c *gin.Context) {
	// Get job ID from URL
	idStr := c.Param("id")
	// Convert id string to ObjectId
	id, err := primitive.ObjectIDFromHex(idStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	// Find job by ObjectId
	var job bson.M
	err = mongoClient.Database("dataStructure").Collection("jobs").FindOne(context.TODO(), bson.D{{"_id", id}}).Decode(&job)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	// Extract the job skill field
	jobSkill, ok := job["job_skills"]
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "job_skill not found"})
		return
	}
	// Return job
	c.JSON(http.StatusOK, gin.H{"job_skill": jobSkill})
}

func getAllCountries(c *gin.Context) {
	cursor, err := mongoClient.Database("dataStructure").Collection("jobs").Find(
		context.TODO(),
		bson.D{},
		options.Find().SetProjection(bson.D{{Key: "search_country", Value: 1}}),
	)

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
	}
	defer cursor.Close(context.TODO())

	getAllUnique(c, cursor, "search_country")
}

func getAllSkills(c *gin.Context) {
	cursor, err := mongoClient.Database("dataStructure").Collection("jobs").Find(
		context.TODO(),
		bson.D{},
		options.Find().SetProjection(bson.D{{Key: "job_skills", Value: 1}}),
	)

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
	}
	defer cursor.Close(context.TODO())

	getAllUnique(c, cursor, "job_skills")
}

func getAllUnique(c *gin.Context, cursor *mongo.Cursor, key string) {

	//check if the data is already in redis
	jobSkills := getFromRedis(c, key)

	fmt.Printf("1")
	if len(jobSkills) == 0 {
		//get it from mongo
		jobSkills = getFromMongo(c, cursor, key)

		fmt.Printf("2")

		//store it in redis
		jobSkillsJSON, err := json.Marshal(jobSkills)

		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		// set key "jobSkills" with value jobSkillsJSON
		status, err := redisClient.Set(context.Background(), key, jobSkillsJSON, 24*time.Second).Result()

		fmt.Printf("Status: %v\n", status)

		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

	}

	c.JSON(http.StatusOK, gin.H{"job_skills": jobSkills})
}

func getFromRedis(c *gin.Context, key string) []string {
	start := time.Now()

	// get value from key "jobSkills"
	jobSkills, err := redisClient.Get(context.Background(), key).Result()

	if err != nil {
		return nil
	}

	// convert to array (from json string)
	var jobSkillsArray []string
	err = json.Unmarshal([]byte(jobSkills), &jobSkillsArray)

	if err != nil {
		return nil
	}

	fmt.Printf("Refis time taken for processing in microseconds: %v\n", time.Since(start).Microseconds())

	return jobSkillsArray
}

func getFromMongo(c *gin.Context, cursor *mongo.Cursor, key string) []string {
	//get exact microsecond for performance measurement (measured in nanoseconds)

	start := time.Now()
	// Define a cursor to iterate over the collection

	// Map to store unique skills
	skillSet := make(map[string]bool)
	var job bson.M
	var ok bool
	var skillsStr string
	//empty array of strings
	var skills = []string{}
	var skill string

	start = time.Now()

	// Iterate through the cursor
	for cursor.Next(context.TODO()) {
		cursor.Decode(&job)
		// Extract job_skill field
		if skillsStr, ok = job[key].(string); ok {
			// Split the string into individual skills
			skills = strings.Split(skillsStr, ", ")
			for _, skill = range skills {
				//check if skill is already in the map
				if !skillSet[skill] {
					skillSet[skill] = true
					//jobSkills = append(jobSkills, skill)
				}
			}
		}
	}
	defer cursor.Close(context.TODO())
	fmt.Printf("Time taken for processing in microseconds: %v\n", time.Since(start).Microseconds())
	start = time.Now()
	//convert map to array
	var jobSkills []string = make([]string, 0, len(skillSet))
	for i := range skillSet {
		jobSkills = append(jobSkills, i)
	}

	fmt.Printf("Time taken for converting in microseconds: %v\n", time.Since(start).Microseconds())

	// Return parsed skills
	return jobSkills
}
