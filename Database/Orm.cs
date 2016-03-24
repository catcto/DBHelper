using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using System.Data.Common;
using System.Configuration;
using System.Reflection;

namespace Cat.Database
{
    [AttributeUsage(AttributeTargets.Struct | AttributeTargets.Property | AttributeTargets.Field)]
    public class DataFieldAttribute : Attribute
    {
        private string _fieldName;

        public string FieldName
        {
            get { return _fieldName; }
            set { _fieldName = value; }
        }

        public DataFieldAttribute(string fieldName)
        {
            this._fieldName = fieldName;
        }
    }

    public class Orm
    {
        public static T GetEntity<T>(DbHelper db, string sql, CommandType CommandType, params DbParameter[] param) where T : new()
        {
            T entity = new T();
            using (DbDataReader reader = db.ExecuteReader(sql, CommandType, param))
            {
                try
                {
                    if (reader.Read())
                    {
                        Type entityType = entity.GetType();
                        PropertyInfo[] propertyInfos = entityType.GetProperties();
                        foreach (PropertyInfo property in propertyInfos)
                        {
                            DataFieldAttribute datafieldAttribute = GetDataFieldAttribute(property);
                            if (datafieldAttribute != null)
                            {
                                if (!(reader[datafieldAttribute.FieldName] is DBNull))
                                {
                                    property.SetValue(entity, reader[datafieldAttribute.FieldName], null);
                                }
                            }
                        }
                    }
                }
                catch
                {
                    throw;
                }
                finally
                {
                    reader.Close();
                    reader.Dispose();
                }
            }
            return entity;
        }

        public static T GetEntity<T>(DbHelper db, string sql, params DbParameter[] param) where T : new()
        {
            return GetEntity<T>(db, sql, CommandType.Text, param);
        }

        public static T GetEntity<T>(string sql, params DbParameter[] param) where T : new()
        {
            return GetEntity<T>(DbHelper.Db, sql, CommandType.Text, param);
        }

        public static T GetEntity<T>(string sql, CommandType CommandType, params DbParameter[] param) where T : new()
        {
            return GetEntity<T>(DbHelper.Db, sql, CommandType, param);
        }

        public static IList<T> GetEntityList<T>(DbHelper db, string sql, CommandType CommandType, params DbParameter[] param) where T : new()
        {
            IList<T> entityList = new List<T>();
            using (DbDataReader reader = db.ExecuteReader(sql, CommandType, param))
            {
                try
                {
                    while (reader.Read())
                    {
                        T entity = new T();
                        Type entityType = entity.GetType();
                        PropertyInfo[] propertyInfos = entityType.GetProperties();
                        foreach (PropertyInfo property in propertyInfos)
                        {
                            DataFieldAttribute datafieldAttribute = GetDataFieldAttribute(property);
                            if (datafieldAttribute != null)
                            {
                                if (!(reader[datafieldAttribute.FieldName] is DBNull))
                                {
                                    property.SetValue(entity, reader[datafieldAttribute.FieldName], null);
                                }
                            }
                        }
                        entityList.Add(entity);
                    }
                }
                catch
                {
                    throw;
                }
                finally
                {
                    reader.Close();
                    reader.Dispose();
                }
            }
            return entityList;
        }

        public static IList<T> GetEntityList<T>(DbHelper db, string sql, params DbParameter[] param) where T : new()
        {
            return GetEntityList<T>(db, sql, CommandType.Text, param);
        }

        public static IList<T> GetEntityList<T>(string sql, CommandType CommandType, params DbParameter[] param) where T : new()
        {
            return GetEntityList<T>(DbHelper.Db, sql, CommandType, param);
        }

        public static IList<T> GetEntityList<T>(string sql, params DbParameter[] param) where T : new()
        {
            return GetEntityList<T>(DbHelper.Db, sql, CommandType.Text, param);
        }

        public static int ExecuteEntity(DbHelper db, string sql, CommandType commandType, object entity)
        {
            return db.ExecuteNonQuery(sql, commandType, GetParameters(db, entity));
        }

        public static int ExecuteEntity(string sql, CommandType commandType, object entity)
        {
            return ExecuteEntity(DbHelper.Db, sql, commandType, entity);
        }

        public static int ExecuteEntity(DbHelper db, string sql, object entity)
        {
            return ExecuteEntity(db, sql, CommandType.Text, entity);
        }

        public static int ExecuteEntity(string sql, object entity)
        {
            return ExecuteEntity(DbHelper.Db, sql, CommandType.Text, entity);
        }

        public static object ExecuteEntityIdentity(DbHelper db, string sql, CommandType commandType, object entity)
        {
            bool ist = db.IsBeginTransaction;
            if (!ist)
            {
                db.BeginTransaction();
            }
            try
            {
                ExecuteEntity(db, sql, commandType, entity);
                return db.ExecuteScalar("SELECT @@IDENTITY");
            }
            catch
            {
                db.RollbackTransaction();
                throw;
            }
            finally
            {
                if (!ist)
                {
                    db.CommitTransaction();
                }
            }
        }

        public static object ExecuteEntityIdentity(DbHelper db, string sql, object entity)
        {
            return ExecuteEntityIdentity(db, sql, CommandType.Text, entity);
        }

        public static object ExecuteEntityIdentity(string sql, CommandType commandType, object entity)
        {
            return ExecuteEntityIdentity(DbHelper.Db, sql, commandType, entity);
        }

        public static object ExecuteEntityIdentity(string sql, object entity)
        {
            return ExecuteEntityIdentity(DbHelper.Db, sql, CommandType.Text, entity);
        }

        private static DbParameter[] GetParameters(DbHelper db, object entity)
        {
            Type entityType = entity.GetType();
            PropertyInfo[] propertyInfos = entityType.GetProperties();
            List<DbParameter> paramerList = new List<DbParameter>();
            foreach (PropertyInfo property in propertyInfos)
            {
                DataFieldAttribute datafieldAttribute = GetDataFieldAttribute(property);
                if (datafieldAttribute != null)
                {
                    object oval = property.GetValue(entity, null);
                    if (oval != null)
                    {
                        DbParameter param = db.Factory.CreateParameter();
                        param.ParameterName = "@" + datafieldAttribute.FieldName;
                        param.Value = oval;
                        paramerList.Add(param);
                    }
                }
            }
            return paramerList.ToArray();
        }

        private static DataFieldAttribute GetDataFieldAttribute(PropertyInfo property)
        {
            object[] oArr = property.GetCustomAttributes(true);
            for (int i = 0; i < oArr.Length; i++)
            {
                if (oArr[i] is DataFieldAttribute)
                    return (DataFieldAttribute)oArr[i];
            }
            return null;
        }
    }
}
