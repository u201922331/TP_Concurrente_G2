package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
)

// Contenido de cada fila
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

// Conversión a int (más sencilla)
func toInt(str string) int {
	result, err := strconv.Atoi(str)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	return result
}

// Fácil lectura del CSV seleccionado
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

// Agregado soporte a Templates/Generics
type Sortables interface {
	int | int16 | int32 | float32 | float64 | string | Registro
}

// Combinar los dos arreglos ingresados y de manera ordenada
func Merge[T Sortables](a []T, b []T, p func(T, T) bool) []T {
	var c []T
	i, j := 0, 0

	var m sync.Mutex // mutex para la sincroinzación segura

	for i < len(a) && j < len(b) {
		if p(a[i], b[j]) {
			c = append(c, a[i])
			i++
		} else {
			c = append(c, b[j])
			j++
		}
	}

	// Bloquear el acceso concurrente a c
	m.Lock()
	defer m.Unlock()

	for ; i < len(a); i++ {
		c = append(c, a[i])
	}
	for ; j < len(b); j++ {
		c = append(c, b[j])
	}

	return c
}

const (
	SIZE_THRESHOLD = (1 << 11)
)

// Punto de entrada. El segundo parámetro sirve para personalizar la función de criterio de ordenamiento
func MergeSort[T Sortables](arr []T, p func(T, T) bool) []T {
	if len(arr) <= 1 {
		return arr
	}

	half := len(arr) / 2
	var l, r []T

	if len(arr) <= SIZE_THRESHOLD { // Secuencial
		l = MergeSort(arr[:half], p)
		r = MergeSort(arr[half:], p)
	} else { // Paralelo
		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			l = MergeSort(arr[:half], p)
		}()

		go func() {
			defer wg.Done()
			r = MergeSort(arr[half:], p)
		}()

		wg.Wait()
	}

	return Merge(l, r, p)
}

// Función de ordenamiento personalizable
func SortByDateAsc(a Registro, b Registro) bool {
	if a.yyyy < b.yyyy {
		return true
	}
	if a.yyyy == b.yyyy && a.mm < b.mm {
		return true
	}
	return false
}

// Punto de entrada
func main() {
	registros := ReadCSV("data/data.csv")
	registros = MergeSort(registros, SortByDateAsc)

	for idx, reg := range registros {
		fmt.Printf("REGISTRO %d: %d/%d\n", idx, reg.mm, reg.yyyy)
	}
}
