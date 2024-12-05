package main

import (
	"context"
	"log"
	"net/http"
	"strings"
	"time"

	"fmt"

	"github.com/gin-gonic/gin"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Your MongoDB Atlas Connection String
const uri = "mongodb+srv://cloudstructuremongo:3zEfmkmoVq92FLdE@cluster0.ekrb5.mongodb.net/?retryWrites=true&w=majority"

// A global variable that will hold a reference to the MongoDB client
var mongoClient *mongo.Client

// The init function will run before our main function to establish a connection to MongoDB. If it cannot connect it will fail and the program will exit.
func init() {
	if err := connect_to_mongodb(); err != nil {
		log.Fatal("Could not connect to MongoDB")
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
	r.GET("/jobs/skills", getAllParsedJobSkills)
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
	err = mongoClient.Database("linkedin").Collection("jobs").FindOne(context.TODO(), bson.D{{"_id", id}}).Decode(&job)
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

func getAllParsedJobSkills(c *gin.Context) {
	fmt.Printf("Yeah")
	//get exact microsecond for performance measurement (measured in nanoseconds)

	start := time.Now()

	var err error
	var cursor *mongo.Cursor
	// Define a cursor to iterate over the collection
	cursor, err = mongoClient.Database("linkedin").Collection("jobs").Find(
		context.TODO(),
		bson.D{},
		options.Find().SetLimit(40000).SetProjection(bson.D{{"job_skills", 1}}),
	)
	
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	defer cursor.Close(context.TODO())

	// Map to store unique skills
	skillSet := make(map[string]bool)
	var job bson.M
	var ok bool
	var skillsStr string
	//empty array of strings
	var skills = []string{}
	var skill string

	fmt.Printf("Time taken for request in microseconds: %v\n", time.Since(start).Microseconds())
	start = time.Now()

	// Iterate through the cursor
	for cursor.Next(context.TODO()) {
		cursor.Decode(&job)
		// Extract job_skill field
		if skillsStr, ok = job["job_skills"].(string); ok {
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
	fmt.Printf("Time taken for processing in microseconds: %v\n", time.Since(start).Microseconds())
	start = time.Now()
	//convert map to array
	var jobSkills []string = make([]string, 0, len(skillSet))
	for i := range skillSet {
		jobSkills = append(jobSkills, i)
	}

	fmt.Printf("Time taken for converting in microseconds: %v\n", time.Since(start).Microseconds())

	// Check if we got any skills
	if len(jobSkills) == 0 {
		c.JSON(http.StatusNotFound, gin.H{"message": "No job skills found"})
		return
	}

	// Return parsed skills
	c.JSON(http.StatusOK, gin.H{"job_skills": jobSkills})
}
