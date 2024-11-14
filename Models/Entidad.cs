#nullable enable // Habilita las características de referencia nula en C#, permitiendo anotaciones y advertencias relacionadas con posibles valores nulos.
using System.Collections.Generic; // Importa el espacio de nombres que contiene clases para manejar colecciones genéricas como Dictionary.

public class Entidad
{
    // Declara un diccionario que almacena pares de claves y valores, donde la clave es un string (nombre de la propiedad) y el valor es un objeto que puede ser null.
    private Dictionary<string, object?> propiedades; 

    // Constructor sin parámetros que inicializa el diccionario de propiedades vacío.
    public Entidad()
    {
        propiedades = new Dictionary<string, object?>(); 
    }

    // Constructor que acepta un diccionario inicial de propiedades.
    // El operador `??` se utiliza para asignar un nuevo diccionario vacío si `initialProperties` es null.
    public Entidad(Dictionary<string, object?> initialProperties)
    {
        propiedades = initialProperties ?? new Dictionary<string, object?>(); 
    }

    // Indexador que permite acceder y modificar las propiedades de la entidad utilizando el nombre de la propiedad como clave.
    // El operador `?` indica que el valor devuelto o asignado puede ser null.
    public object? this[string nombre]
    {
        get
        {
            // `TryGetValue` intenta obtener el valor asociado a la clave especificada (nombre).
            // Si la clave existe, devuelve true y asigna el valor a `value`; si no, devuelve false.
            if (propiedades.TryGetValue(nombre, out var value))
            {
                return value; // Si se encuentra la propiedad, se devuelve su valor.
            }
            return null; // Si la propiedad no existe, se devuelve null.
        }
        set
        {
            // Asigna el valor proporcionado a la clave especificada en el diccionario de propiedades.
            propiedades[nombre] = value;
        }
    }

    // Método que devuelve una copia del diccionario de propiedades.
    // Esto asegura que las propiedades originales no puedan ser modificadas desde fuera de la clase.
    public Dictionary<string, object?> ObtenerPropiedades()
    {
        return new Dictionary<string, object?>(propiedades);
    }
}