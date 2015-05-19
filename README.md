# Hadoop-Tesauros
Programa realizado utilizando el framework Hadoop siguiendo el paradigma Map-Reduce.
El programa debe ser capaz de leer miles de correos electrónicos, agrupar todas las palabras en pares y decir en cuantos documentos diferetes aparecen dichos pares.

## Map
La función **Map** realiza las siguientes funciones:

1. Elimina la cabecera del correo.
2. Elimina símbolos y palabras vacías.
3. Agrupa todas las parejas de palabras (sin repetir).
4. Devuelve como clave cada pareja y como valor un 1.

## Reduce
La función **Reduce** simplemente cuenta el número de claves iguales que les llega y devuelve la clave junto a ese valor.
