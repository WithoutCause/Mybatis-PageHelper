package com.github.pagehelper.dialect;

import com.github.pagehelper.Page;
import com.github.pagehelper.util.MetaObjectUtil;
import com.github.pagehelper.util.SqlUtil;
import org.apache.ibatis.cache.CacheKey;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.session.RowBounds;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author liuzh
 */
public class MySqlDialect extends AbstractDialect {
    public MySqlDialect(SqlUtil sqlUtil) {
        super(sqlUtil);
    }

    @Override
    public Object processPageParameter(MappedStatement ms, Map<String, Object> paramMap, Page page, BoundSql boundSql, CacheKey pageKey) {
        if (page.isFootStoneQuery()) {
            // 如果传入的 pageNum = -1, 直接查最后一页. limit + 1，用于判断是否存在下一页
            paramMap.put(PAGEPARAMETER_FIRST, getOffset(page));
            paramMap.put(PAGEPARAMETER_SECOND, page.getPageSize() + 1);
        } else {
            paramMap.put(PAGEPARAMETER_FIRST, page.getStartRow());
            paramMap.put(PAGEPARAMETER_SECOND, page.getPageSize());
        }
        //处理pageKey
        pageKey.update(page.getStartRow());
        pageKey.update(page.getPageSize());
        //处理参数配置
        if (boundSql.getParameterMappings() != null) {
            List<ParameterMapping> newParameterMappings = new ArrayList<ParameterMapping>();
            if (boundSql != null && boundSql.getParameterMappings() != null) {
                newParameterMappings.addAll(boundSql.getParameterMappings());
            }
            newParameterMappings.add(new ParameterMapping.Builder(ms.getConfiguration(), PAGEPARAMETER_FIRST, Integer.class).build());
            newParameterMappings.add(new ParameterMapping.Builder(ms.getConfiguration(), PAGEPARAMETER_SECOND, Integer.class).build());
            MetaObject metaObject = MetaObjectUtil.forObject(boundSql);
            metaObject.setValue("parameterMappings", newParameterMappings);
        }
        return paramMap;
    }

    private int getOffset(Page page) {
        int pageSize = page.getPageSize();
        int pageNum = page.getPageNum();
        int total = (int) page.getTotal();

        if (pageNum == -1 && page.getTotal() != 0) {
            int pages = getPages(total, pageSize);
            return (pages - 1) * pageSize;
        }

        int queryNum = Math.max((pageNum - 1), 0);
        return queryNum * pageSize;
    }

    private int getPages(int total, int pageSize) {
        int pages = total / pageSize;
        int lastPageSize = total % pageSize;
        if (lastPageSize > 0) {
            pages += 1;
        }
        return pages;
    }

    @Override
    public String getPageSql(String sql, Page page, RowBounds rowBounds, CacheKey pageKey) {
        StringBuilder sqlBuilder = new StringBuilder(sql.length() + 14);
        sqlBuilder.append(sql);
        sqlBuilder.append(" limit ?,?");
        return sqlBuilder.toString();
    }

}
