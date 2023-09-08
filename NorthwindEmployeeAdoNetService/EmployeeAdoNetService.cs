using System;

namespace NorthwindEmployeeAdoNetService;

/// <summary>
/// A service for interacting with the "Employees" table using ADO.NET.
/// </summary>
public sealed class EmployeeAdoNetService
{
    private readonly DbProviderFactory dbFactory;
    private readonly string connectionString;

    /// <summary>
    /// Initializes a new instance of the <see cref="EmployeeAdoNetService"/> class.
    /// </summary>
    /// <param name="dbFactory">The database provider factory used to create database connection and command instances.</param>
    /// <param name="connectionString">The connection string used to establish a database connection.</param>
    /// <exception cref="ArgumentNullException">Thrown when either <paramref name="dbFactory"/> or <paramref name="connectionString"/> is null.</exception>
    /// <exception cref="ArgumentException">Thrown when <paramref name="connectionString"/> is empty or contains only white-space characters.</exception>
    public EmployeeAdoNetService(DbProviderFactory dbFactory, string connectionString)
    {
        if (connectionString is null)
        {
            throw new ArgumentNullException(nameof(connectionString));
        }



        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new ArgumentException($"{nameof(connectionString)} argument is empty or contains only white-space characters.",
                nameof(connectionString));
        }



        this.dbFactory = dbFactory ?? throw new ArgumentNullException(nameof(dbFactory));
        this.connectionString = connectionString;
    }



    /// <summary>
    /// Retrieves a list of all employees from the Employees table of the database.
    /// </summary>
    /// <returns>A list of Employee objects representing the retrieved employees.</returns>
    public IList<Employee> GetEmployees()
    {
        using var connection = dbFactory.CreateConnection();
        connection!.ConnectionString = connectionString;
        connection.Open();
        var command = connection.CreateCommand();
        command.Connection = connection;
        command.CommandText = "SELECT * FROM Employees";
        using var reader = command.ExecuteReader();
        var employees = new List<Employee>();
        while (reader.Read())
        {
            var employee = CreateEmployee(reader);
            employees.Add(employee);
        }
        return employees;
    }



    private static Employee CreateEmployee(DbDataReader dataReader)
    {
        var id = (long)dataReader["EmployeeID"];
        var lastName = (string)dataReader["LastName"];
        var firstName = (string)dataReader["FirstName"];
        var title = dataReader["Title"] is not DBNull ? (string)dataReader["Title"] : null;
        var titleOfCourtesy = dataReader["TitleOfCourtesy"] is not DBNull
            ? (string)dataReader["TitleOfCourtesy"]
            : null;
        var birthDate = dataReader["BirthDate"] is not DBNull ? (string)dataReader["BirthDate"] : null;
        var hireDate = dataReader["HireDate"] is not DBNull ? (string)dataReader["HireDate"] : null;
        var address = dataReader["Address"] is not DBNull ? (string)dataReader["Address"] : null;
        var city = dataReader["City"] is not DBNull ? (string)dataReader["City"] : null;
        var region = dataReader["Region"] is not DBNull ? (string)dataReader["Region"] : null;
        var postalCode = dataReader["PostalCode"] is not DBNull ? (string)dataReader["PostalCode"] : null;
        var country = dataReader["Country"] is not DBNull ? (string)dataReader["Country"] : null;
        var homePhone = dataReader["HomePhone"] is not DBNull ? (string)dataReader["HomePhone"] : null;
        var extension = dataReader["Extension"] is not DBNull ? (string)dataReader["Extension"] : null;
        var notes = dataReader["Notes"] is not DBNull ? (string)dataReader["Notes"] : null;
        var reportsTo = dataReader["ReportsTo"] is not DBNull ? (long?)dataReader["ReportsTo"] : null;
        var photoPath = dataReader["PhotoPath"] is not DBNull ? (string)dataReader["PhotoPath"] : null;



        return new Employee(id)
        {
            LastName = lastName,
            FirstName = firstName,
            Title = title,
            TitleOfCourtesy = titleOfCourtesy,
            BirthDate = birthDate is not null ? DateTime.Parse(birthDate, CultureInfo.InvariantCulture) : null,
            HireDate = hireDate is not null ? DateTime.Parse(hireDate, CultureInfo.InvariantCulture) : null,
            Address = address,
            City = city,
            Region = region,
            PostalCode = postalCode,
            Country = country,
            HomePhone = homePhone,
            Extension = extension,
            Notes = notes,
            ReportsTo = reportsTo,
            PhotoPath = photoPath,
        };
    }


    /// <summary>
    /// Retrieves an employee with the specified employee ID.
    /// </summary>
    /// <param name="employeeId">The ID of the employee to retrieve.</param>
    /// <returns>The retrieved an <see cref="Employee"/> instance.</returns>
    /// <exception cref="EmployeeServiceException">Thrown if the employee is not found.</exception>
    public Employee GetEmployee(long employeeId)
    {
        using (var connection = dbFactory.CreateConnection())
        {

#pragma warning disable CS8602
            connection.ConnectionString = connectionString;
#pragma warning restore CS8602

            connection.Open();

            var command = connection.CreateCommand();
            command.Connection = connection;
            command.CommandText = "SELECT * FROM Employees WHERE EmployeeID = @employeeId";

            var parameter = command.CreateParameter();
            parameter.ParameterName = "@employeeId";
            parameter.DbType = DbType.Int64;
            parameter.Value = employeeId;

            command.Parameters.Add(parameter);

            using (var reader = command.ExecuteReader())
            {
                if (reader.Read())
                {
                    return CreateEmployee(reader);
                }
                else
                {
                    throw new EmployeeServiceException("Employee not found.");
                }
            }
        }
    }
    /// <summary>
    /// Adds a new employee to Employee table of the database.
    /// </summary>
    /// <param name="employee">The  <see cref="Employee"/> object containing the employee's information.</param>
    /// <returns>The ID of the newly added employee.</returns>
    /// <exception cref="EmployeeServiceException">Thrown when an error occurs while adding the employee.</exception>
    public long AddEmployee(Employee employee)
    {
        using (var connection = dbFactory.CreateConnection())
        {
#pragma warning disable CS8602
            connection.ConnectionString = connectionString;
#pragma warning restore CS8602
            connection.Open();

            var transaction = connection.BeginTransaction();

            try
            {
                var command = connection.CreateCommand();
                command.Connection = connection;
                command.Transaction = transaction;

                var fieldNames = typeof(Employee).GetProperties()
                    .Where(p => p.Name != "Id")
                    .Select(p => p.Name);
                var paramNames = fieldNames.Select(fieldName => "@" + fieldName);
                var insertFields = string.Join(", ", fieldNames);
                var insertValues = string.Join(", ", paramNames);

                command.CommandText = $@"
                INSERT INTO Employees (
                    {insertFields}
                )
                VALUES (
                    {insertValues}
                )";

                foreach (var fieldName in fieldNames)
                {
                    var parameter = command.CreateParameter();
                    parameter.ParameterName = "@" + fieldName;
                    var property = typeof(Employee).GetProperty(fieldName);
#pragma warning disable CS8602
                    parameter.Value = property.GetValue(employee) ?? DBNull.Value;
#pragma warning restore CS8602
                    command.Parameters.Add(parameter);
                }

                command.ExecuteNonQuery();

                command.CommandText = "SELECT last_insert_rowid()";
#pragma warning disable CS8605
                var newEmployeeId = (long)command.ExecuteScalar();
#pragma warning restore CS8605

                transaction.Commit();

                return newEmployeeId;
            }
            catch (Exception ex)
            {
                transaction.Rollback();
                throw new EmployeeServiceException("Inserting an employee failed.", ex);
            }
        }
    }


    /// <summary>
    /// Removes an employee from the the Employee table of the database based on the provided employee ID.
    /// </summary>
    /// <param name="employeeId">The ID of the employee to remove.</param>
    /// <exception cref="EmployeeServiceException"> Thrown when an error occurs while attempting to remove the employee.</exception>
    public void RemoveEmployee(long employeeId)
    {
        using var connection = dbFactory.CreateConnection();
#pragma warning disable CS8602
        connection.ConnectionString = connectionString;
#pragma warning restore CS8602
        connection.Open();

        var deleteCommand = connection.CreateCommand();
        deleteCommand.CommandText = "DELETE FROM Employees WHERE EmployeeID = @EmployeeID";

        var parameter = deleteCommand.CreateParameter();
        parameter.ParameterName = "@EmployeeID";
        parameter.Value = employeeId;
        deleteCommand.Parameters.Add(parameter);

        try
        {
            deleteCommand.ExecuteNonQuery();
        }
        catch (Exception ex)
        {
            throw new EmployeeServiceException("Error removing the employee.", ex);
        }
    }

    /// <summary>
    /// Updates an employee record in the Employee table of the database.
    /// </summary>
    /// <param name="employee">The employee object containing updated information.</param>
    /// <exception cref="EmployeeServiceException">Thrown when there is an issue updating the employee record.</exception>
    public void UpdateEmployee(Employee employee)
    {
        using (var connection = dbFactory.CreateConnection())
        {
#pragma warning disable CS8602
            connection.ConnectionString = connectionString;
#pragma warning restore CS8602
            connection.Open();

            var transaction = connection.BeginTransaction();

            try
            {
                var command = connection.CreateCommand();
                command.Connection = connection;
                command.Transaction = transaction;

                var fieldNames = typeof(Employee).GetProperties()
                    .Where(p => p.Name != "Id")
                    .Select(p => p.Name);
                var updateFields = string.Join(", ", fieldNames.Select(fieldName => $"{fieldName} = @{fieldName}"));

                command.CommandText = $@"
                UPDATE Employees
                SET
                    {updateFields}
                WHERE
                    EmployeeID = @Id";

                foreach (var fieldName in fieldNames)
                {
                    var parameter = command.CreateParameter();
                    parameter.ParameterName = "@" + fieldName;
                    var property = typeof(Employee).GetProperty(fieldName);
#pragma warning disable CS8602
                    parameter.Value = property.GetValue(employee) ?? DBNull.Value;
#pragma warning restore CS8602
                    command.Parameters.Add(parameter);
                }

                var idParameter = command.CreateParameter();
                idParameter.ParameterName = "@Id";
                idParameter.Value = employee.Id;
                command.Parameters.Add(idParameter);

                int rowsAffected = command.ExecuteNonQuery();

                if (rowsAffected == 0)
                {
                    throw new EmployeeServiceException("Employee is not updated.");
                }

                transaction.Commit();
            }
            catch (Exception ex)
            {
                transaction.Rollback();
                throw new EmployeeServiceException("Employee update failed.", ex);
            }
        }
    }
}
