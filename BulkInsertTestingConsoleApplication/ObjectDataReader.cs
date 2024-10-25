using System.Data;
using System.Linq.Expressions;
using System.Reflection;

namespace BulkInsertTestingConsoleApplication;

public class ObjectDataReader<TData> : IDataReader
{
    public int Depth { get; }
    
    public int RecordsAffected { get; }
    
    private IEnumerator<TData> dataEnumerator;
    private Func<TData, object>[] accessors;
    private Dictionary<string, int> ordinalLookup;

    public ObjectDataReader(IEnumerable<TData> data)
    {
        this.dataEnumerator = data.GetEnumerator();

        var propertyAccessors = typeof(TData)
            .GetProperties(BindingFlags.Instance | BindingFlags.Public)
            .Where(p => p.CanRead)
            .Select((p, i) => new
            {
                Index = i,
                Property = p,
                Accessor = CreatePropertyAccessor(p)
            })
            .ToArray();

        this.accessors = propertyAccessors.Select(p => p.Accessor).ToArray();
        this.ordinalLookup = propertyAccessors.ToDictionary(
            p => p.Property.Name,
            p => p.Index,
            StringComparer.OrdinalIgnoreCase);
    }
    
    private Func<TData, object> CreatePropertyAccessor(PropertyInfo p)
    {
        var parameter = Expression.Parameter(typeof(TData), "input");
        var propertyAccess = Expression.Property(parameter, p.GetGetMethod());
        var castAsObject = Expression.TypeAs(propertyAccess, typeof(object));
        var lamda = Expression.Lambda<Func<TData, object>>(castAsObject, parameter);
        return lamda.Compile();
    }
    
    public bool Read()
    {
        if (this.dataEnumerator == null)
        {
            throw new ObjectDisposedException("ObjectDataReader");
        }
        return this.dataEnumerator.MoveNext();
    }
    
    // IDataRecord would be responsible for returning values from the columns returned by a database query or the columns in a data table
    
    // This property tells SqlBulkCopy how many columns (or fields) are in each row of data.
    public int FieldCount
    {
        get { return this.accessors.Length; }
    }
    
    // This method allows SqlBulkCopy to find the index (position) of a column by its name.
    public int GetOrdinal(string name)
    {
        int ordinal;
        if (!this.ordinalLookup.TryGetValue(name, out ordinal))
        {
            throw new InvalidOperationException("Unknown parameter name " + name);
        }
        return ordinal;
    }
    
    // GetValue is the most important method as it is the one that SqlBulkCopy calls to get values from each of the properties.
    public object GetValue(int i)
    {
        if (this.dataEnumerator == null)
        {
            throw new ObjectDisposedException("ObjectDataReader");
        }
        return this.accessors[i](this.dataEnumerator.Current);
    }
    
    public void Dispose()
    {
        this.Dispose(true);
        GC.SuppressFinalize(this);
    }

    protected virtual void Dispose(bool disposing)
    {
        if (disposing)
        {
            if (this.dataEnumerator != null)
            {
                this.dataEnumerator.Dispose();
                this.dataEnumerator = null;
            }
        }
    }

    // this is just a wrapper for dispose
    public void Close()
    {
        this.Dispose();
    }
    
    // this just checks  if the data reader is closed/disposed
    public bool IsClosed
    {
        get { return this.dataEnumerator == null; }
    }
    
    // not so necessary methods from here on 
    
    public bool GetBoolean(int i)
    {
        throw new NotImplementedException();
    }

    public byte GetByte(int i)
    {
        throw new NotImplementedException();
    }

    public long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferoffset, int length)
    {
        throw new NotImplementedException();
    }

    public char GetChar(int i)
    {
        throw new NotImplementedException();
    }

    public long GetChars(int i, long fieldoffset, char[]? buffer, int bufferoffset, int length)
    {
        throw new NotImplementedException();
    }

    public IDataReader GetData(int i)
    {
        throw new NotImplementedException();
    }

    public string GetDataTypeName(int i)
    {
        throw new NotImplementedException();
    }

    public DateTime GetDateTime(int i)
    {
        throw new NotImplementedException();
    }

    public decimal GetDecimal(int i)
    {
        throw new NotImplementedException();
    }

    public double GetDouble(int i)
    {
        throw new NotImplementedException();
    }

    public Type GetFieldType(int i)
    {
        throw new NotImplementedException();
    }

    public float GetFloat(int i)
    {
        throw new NotImplementedException();
    }

    public Guid GetGuid(int i)
    {
        throw new NotImplementedException();
    }

    public short GetInt16(int i)
    {
        throw new NotImplementedException();
    }

    public int GetInt32(int i)
    {
        throw new NotImplementedException();
    }

    public long GetInt64(int i)
    {
        throw new NotImplementedException();
    }

    public string GetName(int i)
    {
        throw new NotImplementedException();
    }



    public string GetString(int i)
    {
        throw new NotImplementedException();
    }



    public int GetValues(object[] values)
    {
        throw new NotImplementedException();
    }

    public bool IsDBNull(int i)
    {
        throw new NotImplementedException();
    }

   

    public object this[int i] => throw new NotImplementedException();

    public object this[string name] => throw new NotImplementedException();



    public DataTable? GetSchemaTable()
    {
        throw new NotImplementedException();
    }

    public bool NextResult()
    {
        throw new NotImplementedException();
    }


}