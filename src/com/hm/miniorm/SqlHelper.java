package com.hm.miniorm;

import java.util.List;

public class SqlHelper
{

	public static String putWheres(List<String> wheres, String sql)
	{
		int index = 0;
		while (index < wheres.size())
		{
			String where = wheres.get(index);

			if (index == 0)
			{
				sql = sql + " WHERE " + where;
			}
			else
			{
				sql = sql + " AND " + where;
			}

			index++;
		}
		return sql;
	}

}
