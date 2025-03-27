package main

import (
	"fmt"
	"os"
	"strconv"
)

func main() {
	filePath := "input.txt"

	file, err := os.Create(filePath)
	if err != nil {
		fmt.Printf("Failed to create file: %v\n", err)
		return
	}
	defer file.Close()

	for i := 1; i <= 1000; i++ {
		_, err := file.WriteString(strconv.Itoa(i) + "\n")
		if err != nil {
			fmt.Printf("Failed to write to file: %v\n", err)
			return
		}
	}

	fmt.Println("Successfully wrote numbers to", filePath)
}
