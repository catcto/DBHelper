using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using System.Data.Common;
using System.Configuration;
using System.Reflection;

namespace Cat.Database
{
    public enum DbProviderName
    {
        SqlClient,

        OleDb,

        MySqlClient,

        OracleClient
    }

    public static class DbConfig
    {
        private readonly static string _ConnectionString = ConfigurationManager.ConnectionStrings["ConnectionString"].ConnectionString;
        private readonly static string _ProviderName = ConfigurationManager.ConnectionStrings["ConnectionString"].ProviderName;

        public static string ConnectionString
        {
            get { return _ConnectionString; }
        }

        public static string ProviderName
        {
            get { return _ProviderName; }
        }
    }

    public sealed class DbHelper
    {
        public string ConnectionString { get; set; }

        public string ProviderName { get; set; }

        public DbProviderFactory Factory { get; set; }

        public bool IsBeginTransaction { get; set; }

        public DbTransaction Transaction { get; set; }

        public DbConnection Con { get; set; }

        public DbCommand Cmd { get; set; }

        public DbHelper()
        {
            ConnectionString = DbConfig.ConnectionString;
            ProviderName = DbConfig.ProviderName;
            CreateFactory();
        }

        public static DbHelper Db
        {
            get
            {
                return new DbHelper();
            }
        }

        public DbHelper(string ConnectionString, DbProviderName provider)
        {
            this.ConnectionString = ConnectionString;
            ProviderName = GetProviderName(provider);
            CreateFactory();
        }

        public DbHelper(string ConnectionString, string providerName)
        {
            this.ConnectionString = ConnectionString;
            this.ProviderName = providerName;
            CreateFactory();
        }

        private void CreateFactory()
        {
            Factory = DbProviderFactories.GetFactory(ProviderName);
        }

        public void BeginTransaction()
        {
            try
            {
                Con = Factory.CreateConnection();
                Con.ConnectionString = ConnectionString;
                if (Con.State == ConnectionState.Closed)
                {
                    Con.Open();
                }
                Transaction = Con.BeginTransaction();
                Cmd = Con.CreateCommand();
                Cmd.Transaction = Transaction;
                IsBeginTransaction = true;
            }
            catch
            {
                IsBeginTransaction = false;
                if (Con.State == ConnectionState.Open)
                {
                    Cmd.Dispose();
                    Transaction.Dispose();
                    Con.Close();
                }
                throw;
            }

        }

        public void CommitTransaction()
        {
            try
            {
                if (IsBeginTransaction && Transaction != null)
                {
                    Transaction.Commit();
                }
            }
            catch
            {
                try
                {
                    Transaction.Rollback();
                    throw;
                }
                catch
                {
                    throw;
                }
            }
            finally
            {
                IsBeginTransaction = false;
                if (Con.State == ConnectionState.Open)
                {
                    Cmd.Dispose();
                    Transaction.Dispose();
                    Con.Close();
                }
            }
        }

        public void RollbackTransaction()
        {
            if (IsBeginTransaction)
            {
                Transaction.Rollback();
            }
            IsBeginTransaction = false;
            if (Con.State == ConnectionState.Open)
            {
                Cmd.Dispose();
                Transaction.Dispose();
                Con.Close();
            }
        }

        public void ClearParameters()
        {
            Cmd.Parameters.Clear();
        }

        private string GetProviderName(DbProviderName provider)
        {
            switch (provider)
            {
            case DbProviderName.SqlClient:
                return "System.Data.SqlClient";
            case DbProviderName.OleDb:
                return "System.Data.OleDb";
            case DbProviderName.MySqlClient:
                return "MySql.Data.MySqlClient";
            case DbProviderName.OracleClient:
                return "System.Data.OracleClient";
            default:
                return null;
            }
        }

        public int ExecuteNonQuery(string sql, CommandType CommandType, params DbParameter[] param)
        {
            if (IsBeginTransaction)
            {
                try
                {
                    Cmd.CommandType = CommandType;
                    Cmd.CommandText = sql;
                    Cmd.Parameters.AddRange(param);
                    return Cmd.ExecuteNonQuery();
                }
                catch
                {
                    try
                    {
                        RollbackTransaction();
                        throw;
                    }
                    catch
                    {
                        throw;
                    }
                }

            }
            else
            {
                using (Con = Factory.CreateConnection())
                {
                    using (Cmd = Con.CreateCommand())
                    {
                        try
                        {
                            Con.ConnectionString = ConnectionString;
                            Cmd.CommandType = CommandType;
                            Cmd.CommandText = sql;
                            Cmd.Parameters.AddRange(param);
                            Con.Open();
                            return Cmd.ExecuteNonQuery();
                        }
                        catch
                        {
                            Con.Close();
                            throw;
                        }
                    }
                }
            }
        }

        public int ExecuteNonQuery(string sql, params DbParameter[] param)
        {
            return ExecuteNonQuery(sql, CommandType.Text, param);
        }

        public DbDataReader ExecuteReader(string sql, CommandType CommandType, params DbParameter[] param)
        {
            if (IsBeginTransaction)
            {
                try
                {
                    Cmd.CommandType = CommandType;
                    Cmd.CommandText = sql;
                    Cmd.Parameters.AddRange(param);
                    return Cmd.ExecuteReader();

                }
                catch
                {
                    try
                    {
                        RollbackTransaction();
                        throw;
                    }
                    catch
                    {
                        throw;
                    }
                }

            }
            else
            {
                Con = Factory.CreateConnection();
                using (Cmd = Con.CreateCommand())
                {
                    try
                    {
                        Con.ConnectionString = ConnectionString;
                        Cmd.CommandType = CommandType;
                        Cmd.CommandText = sql;
                        Cmd.Parameters.AddRange(param);
                        Con.Open();
                        return Cmd.ExecuteReader(CommandBehavior.CloseConnection);
                    }
                    catch
                    {
                        Con.Close();
                        throw;
                    }
                }

            }
        }

        public DbDataReader ExecuteReader(string sql, params DbParameter[] param)
        {
            return ExecuteReader(sql, CommandType.Text, param);
        }

        public object ExecuteScalar(string sql, CommandType CommandType, params DbParameter[] param)
        {
            if (IsBeginTransaction)
            {
                try
                {

                    Cmd.CommandType = CommandType;
                    Cmd.CommandText = sql;
                    Cmd.Parameters.AddRange(param);
                    return Cmd.ExecuteScalar();
                }
                catch
                {
                    try
                    {
                        RollbackTransaction();
                        throw;
                    }
                    catch
                    {
                        throw;
                    }
                }

            }
            else
            {
                using (Con = Factory.CreateConnection())
                {
                    using (Cmd = Con.CreateCommand())
                    {
                        try
                        {
                            Con.ConnectionString = ConnectionString;
                            Cmd.CommandType = CommandType;
                            Cmd.CommandText = sql;
                            Cmd.Parameters.AddRange(param);
                            Con.Open();
                            return Cmd.ExecuteScalar();
                        }
                        catch
                        {
                            Con.Close();
                            throw;
                        }
                    }
                }
            }
        }

        public object ExecuteScalar(string sql, params DbParameter[] param)
        {
            return ExecuteScalar(sql, CommandType.Text, param);
        }

        public T ExecuteScalar<T>(string sql, CommandType CommandType, params DbParameter[] param)
        {
            return (T)Convert.ChangeType(ExecuteScalar(sql, CommandType, param), typeof(T));
        }

        public T ExecuteScalar<T>(string sql, params DbParameter[] param)
        {
            return ExecuteScalar<T>(sql, CommandType.Text, param);
        }

        public DataSet GetDataSet(string sql, CommandType CommandType, params DbParameter[] param)
        {
            if (IsBeginTransaction)
            {
                try
                {
                    Cmd.CommandType = CommandType;
                    Cmd.CommandText = sql;
                    Cmd.Parameters.AddRange(param);
                    using (DbDataAdapter da = Factory.CreateDataAdapter())
                    {
                        da.SelectCommand = Cmd;
                        DataSet ds = new DataSet();
                        da.Fill(ds);
                        return ds;
                    }
                }
                catch
                {
                    try
                    {
                        RollbackTransaction();
                        throw;
                    }
                    catch
                    {
                        throw;
                    }
                }

            }
            else
            {
                using (Con = Factory.CreateConnection())
                {
                    using (Cmd = Con.CreateCommand())
                    {
                        try
                        {
                            Con.ConnectionString = ConnectionString;
                            Cmd.CommandType = CommandType;
                            Cmd.CommandText = sql;
                            Cmd.Parameters.AddRange(param);
                            Con.Open();
                            using (DbDataAdapter da = Factory.CreateDataAdapter())
                            {
                                da.SelectCommand = Cmd;
                                DataSet ds = new DataSet();
                                da.Fill(ds);
                                return ds;
                            }
                        }
                        catch
                        {
                            Con.Close();
                            throw;
                        }
                    }
                }
            }
        }

        public DataSet GetDataSet(string sql, params DbParameter[] param)
        {
            return GetDataSet(sql, CommandType.Text, param);
        }

        public DataTable GetDataTable(string sql, CommandType CommandType, params DbParameter[] param)
        {
            if (IsBeginTransaction)
            {
                try
                {
                    Cmd.CommandType = CommandType;
                    Cmd.CommandText = sql;
                    Cmd.Parameters.AddRange(param);
                    using (DbDataAdapter da = Factory.CreateDataAdapter())
                    {
                        da.SelectCommand = Cmd;
                        DataTable dt = new DataTable();
                        da.Fill(dt);
                        return dt;
                    }

                }
                catch
                {
                    try
                    {
                        RollbackTransaction();
                        throw;
                    }
                    catch
                    {
                        throw;
                    }
                }

            }
            else
            {
                using (Con = Factory.CreateConnection())
                {
                    using (Cmd = Con.CreateCommand())
                    {
                        try
                        {
                            Con.ConnectionString = ConnectionString;
                            Cmd.CommandType = CommandType;
                            Cmd.CommandText = sql;
                            Cmd.Parameters.AddRange(param);
                            Con.Open();
                            using (DbDataAdapter da = Factory.CreateDataAdapter())
                            {
                                da.SelectCommand = Cmd;
                                DataTable dt = new DataTable();
                                da.Fill(dt);
                                return dt;
                            }
                        }
                        catch
                        {
                            Con.Close();
                            throw;
                        }
                    }
                }
            }
        }

        public DataTable GetDataTable(string sql, params DbParameter[] param)
        {
            return GetDataTable(sql, CommandType.Text, param);
        }

        public DataRow GetDataRow(string sql, int index, CommandType CommandType, params DbParameter[] param)
        {
            if (IsBeginTransaction)
            {
                try
                {
                    Cmd.CommandType = CommandType;
                    Cmd.CommandText = sql;
                    Cmd.Parameters.AddRange(param);
                    using (DbDataAdapter da = Factory.CreateDataAdapter())
                    {
                        da.SelectCommand = Cmd;
                        DataTable dt = new DataTable();
                        da.Fill(dt);
                        if (dt != null && dt.Rows.Count > 0)
                        {
                            return dt.Rows[index];
                        }
                        return null;
                    }
                }
                catch
                {
                    try
                    {
                        RollbackTransaction();
                        throw;
                    }
                    catch
                    {
                        throw;
                    }
                }

            }
            else
            {
                using (Con = Factory.CreateConnection())
                {
                    using (Cmd = Con.CreateCommand())
                    {
                        try
                        {
                            Con.ConnectionString = ConnectionString;
                            Cmd.CommandType = CommandType;
                            Cmd.CommandText = sql;
                            Cmd.Parameters.AddRange(param);
                            Con.Open();
                            using (DbDataAdapter da = Factory.CreateDataAdapter())
                            {
                                da.SelectCommand = Cmd;
                                DataTable dt = new DataTable();
                                da.Fill(dt);
                                if (dt != null && dt.Rows.Count > 0)
                                {
                                    return dt.Rows[index];
                                }
                                return null;
                            }
                        }
                        catch
                        {
                            Con.Close();
                            throw;
                        }
                    }
                }
            }
        }

        public DataRow GetDataRow(string sql, params DbParameter[] param)
        {
            return GetDataRow(sql, 0, CommandType.Text, param);
        }

        public DataRow GetDataRow(string sql, int index, params DbParameter[] param)
        {
            return GetDataRow(sql, index, CommandType.Text, param);
        }

        public DbParameterCollection ExecuteProc(string procName, params DbParameter[] param)
        {
            if (IsBeginTransaction)
            {
                try
                {
                    Cmd.CommandType = CommandType.StoredProcedure;
                    Cmd.CommandText = procName;
                    Cmd.Parameters.AddRange(param);
                    Cmd.ExecuteNonQuery();
                    return Cmd.Parameters;
                }
                catch
                {
                    try
                    {
                        RollbackTransaction();
                        throw;
                    }
                    catch
                    {
                        throw;
                    }
                }

            }
            else
            {
                using (Con = Factory.CreateConnection())
                {
                    using (Cmd = Con.CreateCommand())
                    {
                        try
                        {
                            Con.ConnectionString = ConnectionString;
                            Cmd.CommandType = CommandType.StoredProcedure;
                            Cmd.CommandText = procName;
                            Cmd.Parameters.AddRange(param);
                            Con.Open();
                            Cmd.ExecuteNonQuery();
                            return Cmd.Parameters;
                        }
                        catch
                        {
                            Con.Close();
                            throw;
                        }
                    }
                }
            }
        }

        public DataSet ExecuteProcDataSet(string procName, params DbParameter[] param)
        {
            if (IsBeginTransaction)
            {
                try
                {
                    Cmd.CommandType = CommandType.StoredProcedure;
                    Cmd.CommandText = procName;
                    Cmd.Parameters.AddRange(param);
                    using (DbDataAdapter da = Factory.CreateDataAdapter())
                    {
                        da.SelectCommand = Cmd;
                        DataSet ds = new DataSet();
                        da.Fill(ds);
                        return ds;
                    }
                }
                catch
                {
                    try
                    {
                        RollbackTransaction();
                        throw;
                    }
                    catch
                    {
                        throw;
                    }
                }

            }
            else
            {
                using (Con = Factory.CreateConnection())
                {
                    using (Cmd = Con.CreateCommand())
                    {
                        try
                        {
                            Con.ConnectionString = ConnectionString;
                            Cmd.CommandType = CommandType.StoredProcedure;
                            Cmd.CommandText = procName;
                            Cmd.Parameters.AddRange(param);
                            using (DbDataAdapter da = Factory.CreateDataAdapter())
                            {
                                da.SelectCommand = Cmd;
                                DataSet ds = new DataSet();
                                da.Fill(ds);
                                return ds;
                            }
                        }
                        catch
                        {
                            Con.Close();
                            throw;
                        }
                    }
                }
            }
        }

        public DataSet ExecuteProcDataSet(string procName, out DbParameterCollection dbpc, params DbParameter[] param)
        {
            if (IsBeginTransaction)
            {
                try
                {
                    Cmd.CommandType = CommandType.StoredProcedure;
                    Cmd.CommandText = procName;
                    Cmd.Parameters.AddRange(param);
                    using (DbDataAdapter da = Factory.CreateDataAdapter())
                    {
                        da.SelectCommand = Cmd;
                        DataSet ds = new DataSet();
                        da.Fill(ds);
                        dbpc = da.SelectCommand.Parameters;
                        return ds;
                    }
                }
                catch
                {
                    try
                    {
                        RollbackTransaction();
                        throw;
                    }
                    catch
                    {
                        throw;
                    }
                }

            }
            else
            {
                using (Con = Factory.CreateConnection())
                {
                    using (Cmd = Con.CreateCommand())
                    {
                        try
                        {
                            Con.ConnectionString = ConnectionString;
                            Cmd.CommandType = CommandType.StoredProcedure;
                            Cmd.CommandText = procName;
                            Cmd.Parameters.AddRange(param);
                            using (DbDataAdapter da = Factory.CreateDataAdapter())
                            {
                                da.SelectCommand = Cmd;
                                DataSet ds = new DataSet();
                                da.Fill(ds);
                                dbpc = da.SelectCommand.Parameters;
                                return ds;
                            }
                        }
                        catch
                        {
                            Con.Close();
                            throw;
                        }
                    }
                }
            }
        }
    }
}
