package main

import (
	"fmt"
	"log"
	"time"

	"github.com/gofiber/fiber/v3"
	"golang.org/x/sync/singleflight"

	"github.com/gofiber/fiber/v3/middleware/logger"
)

var group singleflight.Group

// dbSem simulates a limited DB connection pool (e.g. 5 connections).
var dbSem = make(chan struct{}, 5)

func main() {
	app := fiber.New()
	app.Use(logger.New())

	app.Get("/sf/user/:id", func(c fiber.Ctx) error {
		id := c.Params("id")

		user, err, _ := group.Do(id, func() (any, error) {
			return getUserDB(id)
		})
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).SendString(err.Error())
		}

		return c.JSON(user)
	})

	app.Get("/user/:id", func(c fiber.Ctx) error {
		id := c.Params("id")

		user, err := getUserDB(id)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).SendString(err.Error())
		}

		return c.JSON(user)
	})

	log.Fatal(app.Listen(":3000"))
}

func getUserDB(id string) (map[string]any, error) {
	// simulate a limited connection pool: queue if all 5 "connections" are busy
	dbSem <- struct{}{}
	defer func() { <-dbSem }()

	// simulate db hit
	time.Sleep(2 * time.Second)

	user := map[string]any{
		"id":   id,
		"Name": "Dadang",
	}

	fmt.Printf("Finished fetching user_id: %s\n", id)

	return user, nil
}
