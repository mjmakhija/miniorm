package com.hm.miniorm;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MiniORM
{

	private Connection conn;
	private Logger logger;

	public MiniORM(Connection conn, Logger logger)
	{
		this.conn = conn;
		this.logger = logger;
	}

	public Connection getConn()
	{
		return conn;
	}

	public boolean update(Object object)
	{
		List<String> fieldNames = new ArrayList<>();
		List<Object> fieldValues = new ArrayList<>();
		List<Class> fieldTypes = new ArrayList<>();

		Field[] fields = object.getClass().getDeclaredFields();

		for (Field field : fields)
		{
			field.setAccessible(true);
			try
			{

				Column column = field.getAnnotation(Column.class);

				if (column != null && !column.name().equals("id"))
				{
					fieldNames.add(column.name());
					fieldValues.add(field.get(object));
					fieldTypes.add(field.getType());
				}

			}
			catch (IllegalAccessException e)
			{
				logger.log(Level.SEVERE, e.getMessage(), e);
			}
		}

		Table table = object.getClass().getAnnotation(Table.class);

		String tableName = table.name();

		String sql = "UPDATE " + tableName + " SET";

		for (int i = 0; i < fieldNames.size(); i++)
		{
			String fieldName = fieldNames.get(i);
			if (i == 0)
			{
				sql = sql + " " + fieldName + " = ?";
			}
			else
			{
				sql = sql + ", " + fieldName + " = ?";
			}
		}

		sql = sql + " WHERE id = ?";

		try
		{

			PreparedStatement ps = conn.prepareStatement(sql);
			for (int i = 0; i < fieldNames.size(); i++)
			{
				Class fieldType = fieldTypes.get(i);
				Object fieldValue = fieldValues.get(i);

				setValueInPreparedStatement(ps, i + 1, fieldType, fieldValue);

			}
			// Set value for id at last
			Field idField = object.getClass().getDeclaredField("id");
			idField.setAccessible(true);
			ps.setInt(fieldNames.size() + 1, (int) idField.get(object));

			ps.executeUpdate();

			return true;
		}
		catch (IllegalAccessException | IllegalArgumentException | NoSuchFieldException | SecurityException | SQLException e)
		{
			logger.log(Level.SEVERE, e.getMessage(), e);
			return false;
		}

	}

	public boolean insert(Object object)
	{
		List<String> fieldNames = new ArrayList<>();
		List<Object> fieldValues = new ArrayList<>();
		List<Class> fieldTypes = new ArrayList<>();

		Field[] fields = object.getClass().getDeclaredFields();

		for (Field field : fields)
		{
			field.setAccessible(true);
			try
			{

				Column column = field.getAnnotation(Column.class);

				if (column != null && !column.name().equals("id"))
				{
					fieldNames.add(column.name());
					fieldValues.add(field.get(object));
					fieldTypes.add(field.getType());
				}

			}
			catch (IllegalAccessException e)
			{
				logger.log(Level.SEVERE, e.getMessage(), e);
			}
		}

		Table table = object.getClass().getAnnotation(Table.class);

		String tableName = table.name();

		String sql = "INSERT INTO " + tableName + " (";
		for (int i = 0; i < fieldNames.size(); i++)
		{
			String fieldName = fieldNames.get(i);
			if (i == 0)
			{
				sql = sql + " " + fieldName;
			}
			else
			{
				sql = sql + ", " + fieldName;
			}
		}
		sql += ") VALUES(";
		for (int i = 0; i < fieldNames.size(); i++)
		{
			if (i == 0)
			{
				sql = sql + "?";
			}
			else
			{
				sql = sql + ", ?";
			}
		}
		sql += ");";

		try
		{

			PreparedStatement ps = conn.prepareStatement(sql);
			for (int i = 0; i < fieldNames.size(); i++)
			{
				Class fieldType = fieldTypes.get(i);
				Object fieldValue = fieldValues.get(i);

				setValueInPreparedStatement(ps, i + 1, fieldType, fieldValue);
			}

			ps.executeUpdate();

			long last_inserted_id = 0;
			ResultSet rs = ps.getGeneratedKeys();
			if (rs.next())
			{
				last_inserted_id = rs.getInt(1);
			}

			Field idField = object.getClass().getDeclaredField("id");
			idField.setAccessible(true);
			idField.set(object, (int) last_inserted_id);

			return true;
		}
		catch (IllegalAccessException | IllegalArgumentException | NoSuchFieldException | SecurityException | SQLException e)
		{
			logger.log(Level.SEVERE, e.getMessage(), e);
			return false;
		}

	}

	public <E> List<E> get(Class<E> classOfE, String sql)
	{
		//if there are primitive classes
		if (classOfE.equals(Integer.class)
				|| classOfE.equals(BigDecimal.class)
				|| classOfE.equals(String.class))
		{
			return getSingleResult(classOfE, sql);
		}

		List<E> result = new ArrayList<>();
		Field[] fields = classOfE.getDeclaredFields();
		try
		{

			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sql);

			while (rs.next())
			{

				// New object to add in array list
				E obj = classOfE.newInstance();

				for (Field field : fields)
				{

					field.setAccessible(true);

					// Get annotation of field
					Column column = field.getAnnotation(Column.class);

					Class<?> fieldDataType = field.getType();

					Object value = null;

					if (fieldDataType.equals(Integer.TYPE))
					{
						value = rs.getInt(column.name());
					}
					else if (fieldDataType.equals(Integer.class))
					{
						int nValue = rs.getInt(column.name());
						value = rs.wasNull() ? null : nValue;
					}
					else if (fieldDataType.equals(Double.TYPE))
					{
						value = rs.getDouble(column.name());
					}
					else if (fieldDataType.equals(Double.class))
					{
						Double nValue = rs.getDouble(column.name());
						value = rs.wasNull() ? null : nValue;
					}
					else if (fieldDataType.equals(String.class))
					{
						String nValue = rs.getString(column.name());
						value = rs.wasNull() ? null : nValue;
					}
					else if (fieldDataType.equals(Boolean.TYPE))
					{
						value = rs.getInt(column.name()) > 0;
					}
					else if (fieldDataType.equals(Boolean.class))
					{
						value = rs.getInt(column.name()) > 0;
					}
					else if (fieldDataType.equals(Date.class))
					{
						String nValue = rs.getString(column.name());
						value = rs.wasNull() ? null : new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(nValue);
					}
					else if (fieldDataType.equals(BigDecimal.class))
					{
						String str = rs.getString(column.name());
						value = rs.wasNull() ? null : new BigDecimal(str);
					}

					field.set(obj, value);

				}

				result.add(obj);

			}

			rs.close();
			stmt.close();

		}
		catch (IllegalAccessException | InstantiationException | SQLException | ParseException ex)
		{
			logger.log(Level.SEVERE, ex.getMessage(), ex);
		}

		return result;

	}

	private <E> List<E> getSingleResult(Class<E> classOfE, String sql)
	{
		List<E> result = new ArrayList<>();
		try
		{

			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sql);

			while (rs.next())
			{

				// New object to add in array list
				E obj;

				Object value = null;

				value = getValueFromRS(classOfE, rs, 1);

				obj = (E) value;

				result.add(obj);

			}

			rs.close();
			stmt.close();

		}
		catch (SQLException | ParseException ex)
		{
			logger.log(Level.SEVERE, ex.getMessage(), ex);
		}

		return result;
	}

	public boolean delete(Object object)
	{
		try
		{
			Table table = object.getClass().getAnnotation(Table.class);
			Field field = object.getClass().getDeclaredField("id");
			field.setAccessible(true);

			String sqlDelete = "DELETE FROM %s WHERE id=%s";
			String sql = String.format(sqlDelete, table.name(), field.get(object));

			PreparedStatement ps = conn.prepareStatement(sql);

			ps.executeUpdate();
			return true;

		}
		catch (NoSuchFieldException | SecurityException | SQLException | IllegalArgumentException | IllegalAccessException ex)
		{
			logger.log(Level.SEVERE, ex.getMessage(), ex);
			return false;
		}

	}

	public boolean execute(String sql)
	{
		try
		{
			PreparedStatement ps = conn.prepareStatement(sql);
			ps.executeUpdate();
			return true;

		}
		catch (SecurityException | SQLException ex)
		{
			logger.log(Level.SEVERE, ex.getMessage(), ex);
			return false;
		}
	}

	private <T> void setValueInPreparedStatement(PreparedStatement ps, int index, Class<T> fieldType, Object fieldValue) throws SQLException
	{
		if (fieldValue == null)
		{
			if (fieldType.equals(Integer.TYPE)
					|| fieldType.equals(Integer.class))
			{
				ps.setNull(index, Types.INTEGER);
			}
			else if (fieldType.equals(String.class))
			{
				ps.setNull(index, Types.VARCHAR);
			}
			else if (fieldType.equals(Boolean.TYPE))
			{
				ps.setNull(index, Types.INTEGER);
			}
			else if (fieldType.equals(Date.class))
			{
				ps.setNull(index, Types.VARCHAR);
			}
			else if (fieldType.equals(BigDecimal.class))
			{
				ps.setNull(index, Types.NUMERIC);
			}
		}
		else
		{
			if (fieldType.equals(Integer.TYPE)
					|| fieldType.equals(Integer.class))
			{
				ps.setInt(index, (Integer) fieldValue);
			}
			else if (fieldType.equals(String.class))
			{
				ps.setString(index, (String) fieldValue);
			}
			else if (fieldType.equals(Boolean.TYPE))
			{
				ps.setBoolean(index, (boolean) fieldValue);
			}
			else if (fieldType.equals(Date.class))
			{
				ps.setString(index, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format((Date) fieldValue));
			}
			else if (fieldType.equals(BigDecimal.class))
			{
				ps.setString(index, fieldValue.toString());
			}
		}
	}

	private <E> Object getValueFromRS(Class<E> classOfE, ResultSet rs, int index) throws SQLException, ParseException
	{
		Object value = null;
		if (classOfE.equals(Integer.TYPE))
		{
			value = rs.getInt(index);
		}
		else if (classOfE.equals(Integer.class))
		{
			int nValue = rs.getInt(index);
			value = rs.wasNull() ? null : nValue;
		}
		else if (classOfE.equals(String.class))
		{
			String nValue = rs.getString(index);
			value = rs.wasNull() ? null : nValue;
		}
		else if (classOfE.equals(Boolean.TYPE))
		{
			value = rs.getInt(index) > 0;
		}
		else if (classOfE.equals(Boolean.class))
		{
			value = rs.getInt(index) > 0;
		}
		else if (classOfE.equals(Date.class))
		{
			String nValue = rs.getString(index);
			value = rs.wasNull() ? null : new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(nValue);
		}
		else if (classOfE.equals(BigDecimal.class))
		{
			String str = rs.getString(index);
			value = rs.wasNull() ? null : new BigDecimal(str);
		}
		return value;
	}

}
