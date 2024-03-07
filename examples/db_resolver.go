package main

import (
	"fmt"
)

type Person struct {
	Name     string `db:"name"`
	Age      int    `json:"age"` // No db tag here
	Location string // No db tag here
}

func main() {
	// Example with a slice of structs
	type Person struct {
		Name string
		Age  int
	}
	people := []Person{{"Alice", 30}, {"Bob", 35}}

	fmt.Println("Keys from slice of structs:")
	fmt.Println(InsertQuery("person", people))

	// Example with a single struct
	person := Person{"Charlie", 25}
	fmt.Println("Keys from single struct:")
	fmt.Println(InsertQuery("person", person))

	// Example with a map
	ages := []map[string]int{
		{"Alice": 30, "Bob": 35},
	}
	fmt.Println("Keys from map:")
	fmt.Println(InsertQuery("person", ages))
}
