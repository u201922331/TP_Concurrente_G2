package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// This is what a row contains
type Registro struct {
	yyyy                  int
	mm                    int
	region                string
	provincia             string
	ubigeo_distrito       string
	distrito              string
	cod_unidad_ejecutora  string
	desc_unidad_ejecutora string
	cod_ipress            string
	ipress                string
	nivel_eess            string
	plan_de_seguro        string
	cod_servicio          string
	desc_servicio         string
	sexo                  string
	grupo_edad            string
	atenciones            int
}

// Helper function to convert string to int
func toInt(str string) int {
	result, err := strconv.Atoi(str)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	return result
}

// Easily read the dataset
func ReadCSV(filename string) []Registro {
	file, err := os.Open(filename)

	registros := []Registro{}

	if err != nil {
		fmt.Println(err.Error())
		return registros
	}

	defer file.Close()

	scanner := bufio.NewScanner(file)
	i := 0
	for scanner.Scan() {
		fields := strings.Split(scanner.Text(), "|")

		if i > 0 {
			registros = append(registros, Registro{
				toInt(fields[0]),
				toInt(fields[1]),
				fields[2],
				fields[3],
				fields[4],
				fields[5],
				fields[6],
				fields[7],
				fields[8],
				fields[9],
				fields[10],
				fields[11],
				fields[12],
				fields[13],
				fields[14],
				fields[15],
				toInt(fields[16]),
			})
		}

		i++
	}
	return registros
}

// Templates/Generics support
type Sortables interface {
	int | int16 | int32 | float32 | float64 | string | Registro
}

// Merge two arrays together
func Merge[T Sortables](a []T, b []T, p func(T, T) bool) []T {
	var c []T
	i, j := 0, 0

	for i < len(a) && j < len(b) {
		if p(a[i], b[j]) {
			c = append(c, a[i])
			i++
		} else {
			c = append(c, b[j])
			j++
		}
	}
	for ; i < len(a); i++ {
		c = append(c, a[i])
	}
	for ; j < len(b); j++ {
		c = append(c, b[j])
	}
	return c
}

// Entry point. First parameter is the array/slice to sort, the second one works as a comparison function
func MergeSort[T Sortables](arr []T, p func(T, T) bool) []T {
	if len(arr) <= 1 {
		return arr
	}

	half := len(arr) / 2
	l := MergeSort(arr[:half], p)
	r := MergeSort(arr[half:], p)

	return Merge(l, r, p)
}

func SortByDateAsc(a Registro, b Registro) bool {
	if a.yyyy < b.yyyy {
		return true
	}
	if a.yyyy == b.yyyy && a.mm < b.mm {
		return true
	}
	return false
}

func main() {
	registros := ReadCSV("data/data.csv")
	registros = MergeSort(registros, SortByDateAsc) // TODO: GoRoutines

	for idx, reg := range registros {
		fmt.Printf("REGISTRO %d: %d/%d\n", idx, reg.mm, reg.yyyy)
	}
}
