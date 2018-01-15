using Npgsql;
using System;
using System.Data;
using System.Linq;
using System.Xml;
using System.Xml.Linq;
using System.Data;
using System.Data.SqlClient;
using System.Text;
using System.Collections.Generic;

namespace FiasImport
{
	static class Extention
	{
		public static void ForeachAndForeachExceptLast<T>(this IEnumerable<T> source, Action<T> forEach, Action<T> forEachExceptLast = null)
		{
			using (var enumerator = source.GetEnumerator())
			{
				if (enumerator.MoveNext())
				{
					var item = enumerator.Current; ;

					while (enumerator.MoveNext())
					{
						forEach(item);
						if (forEachExceptLast != null)
							forEachExceptLast(item);
						item = enumerator.Current;
					}
					forEach(item);
				}
			}
		}
	}
	
	class Program
	{
		public static string GenerateInsertStatement(string table, Dictionary<string, object> data)
		{
			StringBuilder statement = new StringBuilder();

			statement.Append($"INSERT INTO public.\"{table}\" (");

			data.ForeachAndForeachExceptLast(column =>
			{
				statement.Append($"\"{column.Key}\"");
			},
				column =>
				{
					statement.Append(", ");
				});

			statement.Append($") VALUES(");

			data.ForeachAndForeachExceptLast(column =>
			{
				statement.Append($"\'{column.Value}\'");
			},
				column =>
				{
					statement.Append(", ");
				});

			statement.Append($")");

			statement.Append($" RETURNING \"RecID\"");

			return statement.ToString();
		}


		static void Main(string[] args)
		{
			XmlReader reader = XmlReader.Create(@"AS_ADDROBJ_20171217_33bb6037-d55f-49e1-bb44-b24e834a7ff5.XML");

			NpgsqlConnection conn = new NpgsqlConnection("server=localhost;port=5432;user id=postgres;password=postgres;database=fias");
			conn.Open();

			IDbCommand command = conn.CreateCommand();



			// Define a query
			//NpgsqlCommand command = new NpgsqlCommand("SELECT city, state FROM cities", conn);

			// Execute the query and obtain a result set
			//NpgsqlDataReader dr = command.ExecuteReader();

			// Output rows
			//while (dr.Read())
			//	Console.Write("{0}\t{1} \n", dr[0], dr[1]);



			//XmlDocument document = new XmlDocument();
			//document.Load(@"AS_ADDROBJ_20171217_33bb6037-d55f-49e1-bb44-b24e834a7ff5.XML");
			//Console.WriteLine(document.ChildNodes.Count);
			

			var dict = new Dictionary<string, object>();
			var count = 0;
			while (reader.Read())
			{
				if (reader.IsStartElement())
				{
					if (reader.IsEmptyElement) { }
					//Console.WriteLine("<{0}/>", reader.Name);
					else
					{
						//Console.Write("<{0}> ", reader.Name);
						reader.Read(); // Read the start tag.
						if (reader.IsStartElement()) { }  // Handle nested elements.
														  //Console.Write("\r\n<{0}>", reader.Name);
														  //Console.WriteLine(reader.ReadString());  //Read the text content of the element.
					}

					if (reader.HasAttributes)
					{
						//Console.WriteLine("Attributes of <" + reader.Name + ">");
						
						while (reader.MoveToNextAttribute())
						{
							dict.Add(reader.Name, reader.Value);
							//Console.WriteLine(" {0}={1}", reader.Name, reader.Value);
						}

						var sql = GenerateInsertStatement("ADDROBJ", dict);
						dict.Clear();

						command.CommandText = sql;
						var rows = command.ExecuteNonQuery();
						count += rows;
						Console.WriteLine($"ROWS: {count.ToString()}");

						// Move the reader back to the element node.
						reader.MoveToElement();
					}
				}

				//Console.WriteLine(result);
			}
			
			conn.Close();
			Console.WriteLine("END");
			Console.ReadLine();
		}
	}
}
