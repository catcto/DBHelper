using System;
using System.Collections.Generic;
using System.Text;
using System.Data.Common;

namespace Cat.Database
{
    public class DbInsert
    {
        private StringBuilder field = new StringBuilder();
        private StringBuilder values = new StringBuilder();
        private List<DbParameter> pars = new List<DbParameter>();
        private DbHelper dataBase;

        public DbInsert()
        {
            dataBase = new DbHelper();
        }

        public DbInsert(DbHelper db)
        {
            dataBase = db;
        }

        public void Add(string name, object value)
        {
            field.Append(",[" + name + "]");
            values.Append("," + "@" + name);
            DbParameter param = dataBase.Factory.CreateParameter();
            param.ParameterName = "@" + name;
            param.Value = value;
            pars.Add(param);
        }

        public int Execute(string tableName)
        {
            return dataBase.ExecuteNonQuery("INSERT INTO [" + tableName + "](" + field.ToString().TrimStart(',') + ") VALUES(" + values.ToString().TrimStart(',') + ")", pars.ToArray());
        }

        public object ExecuteIdentity(string tableName)
        {
            bool ist = dataBase.IsBeginTransaction;
            if (!ist)
            {
                dataBase.BeginTransaction();
            }
            try
            {
                Execute(tableName);
                return dataBase.ExecuteScalar("SELECT @@IDENTITY");
            }
            catch
            {
                dataBase.RollbackTransaction();
                throw;
            }
            finally
            {
                if (!ist)
                {
                    dataBase.CommitTransaction();
                }
            }
        }
    }
}
