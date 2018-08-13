package com.company.platform.team.projpatternreco.stormtopology.utils;

import com.company.platform.team.projpatternreco.stormtopology.data.DBProjectPatternNode;
import com.company.platform.team.projpatternreco.stormtopology.data.DBProject;
import com.company.platform.team.projpatternreco.stormtopology.data.MysqlMapper;
import org.apache.ibatis.datasource.pooled.PooledDataSource;
import org.apache.ibatis.datasource.unpooled.UnpooledDataSource;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.apache.ibatis.transaction.TransactionFactory;
import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Created by admin on 2018/8/8.
 */
public class MysqlUtil {
    private static Logger logger = LoggerFactory.getLogger(MysqlUtil.class);
    private static final String DRIVER = "com.mysql.cj.jdbc.Driver";
    private static final String URL_PARAMETERS = "useUnicode=true&characterEncoding=utf-8&useSSL=false&serverTimezone=UTC";

    private SqlSessionFactory sqlSessionFactory;
    private static MysqlUtil instance;

    private MysqlUtil(Map config) {
        String url = config.get("url").toString() + "?" + URL_PARAMETERS;
        String userName = config.get("userName").toString();
        String password = config.get("password").toString();
        boolean pooled;
        try {
            pooled = Boolean.getBoolean(config.get("pooled").toString());
        } catch (Exception e) {
            pooled = false;
            logger.warn("parse pooled from config failed, use default value: " + pooled);
        }

        Configuration configuration = getConfiguration(url, userName, password, pooled);
        sqlSessionFactory = new SqlSessionFactoryBuilder().build(configuration);
    }

    public static synchronized MysqlUtil getInstance(Map conf) {
        if (instance == null) {
            instance = new MysqlUtil(conf);
        }
        return instance;
    }

    public boolean refreshProjectNodes(int projectId, List<DBProjectPatternNode> nodes)
    {
        SqlSession session = sqlSessionFactory.openSession();
        boolean success = false;
        if (projectId > 0 && nodes != null) {
            try {
                MysqlMapper mapper = session.getMapper(MysqlMapper.class);
                mapper.deleteProjectNodes(projectId);
                mapper.insertProjectNodes(nodes);
                session.commit();
                success = true;
            } catch (Exception e) {
                session.rollback();
                logger.error("refresh nodes for project: " + projectId + " failed.", e);
            } finally {
                session.close();
            }
        } else {
            logger.warn("invalid project Id or nodes is null.");
        }
        return success;
    }
    public int insertNodes(List<DBProjectPatternNode> nodes) {
        SqlSession session = sqlSessionFactory.openSession();
        int successRows = -1;
        if (nodes != null) {
            try {
                MysqlMapper mapper = session.getMapper(MysqlMapper.class);
                successRows = mapper.insertProjectNodes(nodes);
                session.commit();
            } catch (Exception e) {
                successRows = -1;
                session.rollback();
                logger.error("insert nodes failed.", e);
            } finally {
                session.close();
            }
        } else {
            logger.warn("nodes is null.");
        }
        return successRows;
    }
    public List<DBProjectPatternNode> getProjectLeaves(String projectName) {
       SqlSession session = sqlSessionFactory.openSession();
       List<DBProjectPatternNode> nodes = null;
       try {
           MysqlMapper mapper = session.getMapper(MysqlMapper.class);
           nodes = mapper.selectProjectLeaves(projectName);
       } catch (Exception e) {
           logger.error("get leaves for project: " + projectName + " failed.", e);
        } finally {
           session.close();
       }
       return nodes;
    }
    public int updateParentNode(DBProjectPatternNode node) {
        SqlSession session = sqlSessionFactory.openSession();
        int successRows = -1;
        try {
            MysqlMapper mapper = session.getMapper(MysqlMapper.class);
            successRows = mapper.updateParentNode(node.getProjectId(), node.getPatternLevel(), node.getPatternKey(), node.getParentKey());
            session.commit();
        } catch (Exception e) {
            successRows = -1;
            session.rollback();
            logger.error("update node failed.", e);
        } finally {
            session.close();
        }
        return successRows;
    }
    public List<DBProject> getProjectMetas() {
        SqlSession session = sqlSessionFactory.openSession();
        List<DBProject> projects = null;
        try {
            MysqlMapper mapper = session.getMapper(MysqlMapper.class);
            projects = mapper.selectProjectMetas();
        } catch (Exception e) {
            logger.error("get projects info failed.", e);
        } finally {
            session.close();
        }
        return projects;
    }
    public DBProject getProjectMeta(String projectName){
        SqlSession session = sqlSessionFactory.openSession();
        DBProject project = null;
        try {
            MysqlMapper mapper = session.getMapper(MysqlMapper.class);
            project = mapper.selectProjectMeta(projectName);
        } catch (Exception e) {
            logger.error("get info for project " + projectName + " failed.", e);
        } finally {
            session.close();
        }
        return project;
    }

    private Configuration getConfiguration(String url, String userName, String password, boolean pooled) {
        if (url != null && userName != null && password != null) {
            TransactionFactory transactionFactory = new JdbcTransactionFactory();
            Environment environment;
            if (pooled) {
                PooledDataSource ds = new PooledDataSource();
                ds.setDriver(DRIVER);
                ds.setUrl(url);
                ds.setUsername(userName);
                ds.setPassword(password);
                environment = new Environment("development", transactionFactory, ds);
            } else {
                UnpooledDataSource ds = new UnpooledDataSource();
                ds.setDriver(DRIVER);
                ds.setUrl(url);
                ds.setUsername(userName);
                ds.setPassword(password);
                environment = new Environment("development", transactionFactory, ds);
            }
            Configuration configuration = new Configuration(environment);
            configuration.addMapper(MysqlMapper.class);
            return configuration;
        }
        return null;
    }
}
