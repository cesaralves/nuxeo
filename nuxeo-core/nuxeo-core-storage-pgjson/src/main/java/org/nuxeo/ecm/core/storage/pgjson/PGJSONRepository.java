/*
 * (C) Copyright 2018 Nuxeo (http://nuxeo.com/) and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Contributors:
 *     Florent Guillaume
 */
package org.nuxeo.ecm.core.storage.pgjson;

import static org.nuxeo.ecm.core.storage.dbs.DBSDocument.KEY_ID;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.resource.spi.ConnectionManager;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.nuxeo.ecm.core.api.Lock;
import org.nuxeo.ecm.core.api.NuxeoException;
import org.nuxeo.ecm.core.api.PartialList;
import org.nuxeo.ecm.core.api.ScrollResult;
import org.nuxeo.ecm.core.model.Repository;
import org.nuxeo.ecm.core.query.sql.model.OrderByClause;
import org.nuxeo.ecm.core.storage.State;
import org.nuxeo.ecm.core.storage.State.StateDiff;
import org.nuxeo.ecm.core.storage.dbs.DBSDocument;
import org.nuxeo.ecm.core.storage.dbs.DBSExpressionEvaluator;
import org.nuxeo.ecm.core.storage.dbs.DBSRepositoryBase;
import org.nuxeo.ecm.core.storage.dbs.DBSTransactionState.ChangeTokenUpdater;
import org.nuxeo.runtime.datasource.ConnectionHelper;

/**
 * PostgreSQL+JSON implementation of a {@link Repository}.
 *
 * @since 10.10
 */
public class PGJSONRepository extends DBSRepositoryBase {

    private static final Logger log = LogManager.getLogger(PGJSONRepository.class);

    protected static final int BATCH_SIZE = 100;

    protected static final String TABLE_NAME = "documents";

    protected static final String ID_COL = "id";

    protected static final String JSON_COL = "doc";

    protected PGJSONConverter converter;

    protected Connection connection;

    public PGJSONRepository(ConnectionManager cm, PGJSONRepositoryDescriptor descriptor) {
        super(cm, descriptor.name, descriptor);
        converter = new PGJSONConverter();
        String repositoryName = descriptor.name;
        String dataSourceName = getDataSourceName(repositoryName);
        try {
            connection = ConnectionHelper.getConnection(dataSourceName, true);
            connection.setClientInfo("ApplicationName", "Nuxeo DBS");
        } catch (SQLException e) {
            throw new NuxeoException(e);
        }
        initRepository();
    }

    @Override
    public void shutdown() {
        super.shutdown();
        try {
            connection.close();
        } catch (SQLException e) {
            log.error("Failed to close connection", e);
        }
    }

    protected static String getDataSourceName(String repositoryName) {
        // use same convention as VCS for the datasource of a given repository
        return "repository_" + repositoryName;
    }

    @Override
    public List<IdType> getAllowedIdTypes() {
        return Collections.singletonList(IdType.varchar);
    }

    protected void initRepository() {
        try {
            // check table
            DatabaseMetaData metadata = connection.getMetaData();
            boolean hasTable = false;
            try (ResultSet rs = metadata.getTables(null, null, TABLE_NAME, new String[] { "TABLE" })) {
                while (rs.next()) {
                    String tableName = rs.getString("TABLE_NAME");
                    if (TABLE_NAME.equals(tableName)) {
                        hasTable = true;
                        break;
                    }
                }
            }
            if (hasTable) {
                return;
            }
            String sql = "CREATE TABLE " + TABLE_NAME + " (" + ID_COL + " varchar(36) PRIMARY KEY, " + JSON_COL
                    + " jsonb)";
            try (Statement st = connection.createStatement()) {
                log.trace("SQL: {}", sql);
                st.execute(sql);
            }
            String indexSQL = "";
            initRoot();
        } catch (SQLException e) {
            throw new NuxeoException(e);
        }
    }

    @Override
    public String generateNewId() {
        // TODO Auto-generated method stub
        // return null;
        throw new UnsupportedOperationException();
    }

    @Override
    public State readState(String id) {
        // TODO Auto-generated method stub
        // return null;
        throw new UnsupportedOperationException();
    }

    @Override
    public List<State> readStates(List<String> ids) {
        // TODO Auto-generated method stub
        // return null;
        throw new UnsupportedOperationException();
    }

    @Override
    public void createState(State state) {
        createStates(Collections.singletonList(state));
    }

    @Override
    public void createStates(List<State> states) {
        boolean batched = states.size() > 1;
        try {
            String sql = "INSERT INTO " + TABLE_NAME + "(" + ID_COL + ", " + JSON_COL + ") VALUES (?, ?::jsonb)";
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                int batch = 0;
                for (Iterator<State> it = states.iterator(); it.hasNext();) {
                    State state = it.next();
                    String id = (String) state.get(KEY_ID);
                    String json = converter.stateToJson(state);
                    ps.setString(1, id);
                    ps.setString(2, json);
                    if (batched) {
                        ps.addBatch();
                        batch++;
                        if (batch % BATCH_SIZE == 0 || !it.hasNext()) {
                            ps.executeBatch();
                        }
                    } else {
                        ps.execute();
                    }
                }
            }
        } catch (SQLException e) {
            throw new NuxeoException(e);
        }
    }

    @Override
    public void updateState(String id, StateDiff diff, ChangeTokenUpdater changeTokenUpdater) {
        // TODO changeTokenUpdater
        // TODO optimize later the number of writes
        try {
            String sql = "UPDATE " + TABLE_NAME + " SET " + JSON_COL + " = ..... WHERE " + ID_COL + " = ?";
            try (PreparedStatement ps = connection.prepareStatement(sql)) {

            }
        } catch (SQLException e) {
            throw new NuxeoException(e);
        }
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteStates(Set<String> ids) {
        if (ids.isEmpty()) {
            return;
        }
        try {
            String sql = "DELETE FROM " + TABLE_NAME + " WHERE " + ID_COL + " ";
            int size = ids.size();
            if (size == 1) {
                sql += "= ?";
            } else {
                StringBuilder buf = new StringBuilder(3 + 3 * size);
                buf.append("IN (");
                for (int i = 0; i < size; i++) {
                    if (i != 0) {
                        buf.append(", ");
                    }
                    buf.append('?');
                }
                buf.append(')');
                sql += buf;
            }
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                int i = 0;
                for (String id : ids) {
                    ps.setString(++i, id);
                }
                int count = ps.executeUpdate();
                if (count != ids.size()) {
                    log.debug("Removed {} docs for {} ids: {}", () -> count, ids::size, () -> ids);
                }
            }
        } catch (SQLException e) {
            throw new NuxeoException(e);
        }
    }

    @Override
    public State readChildState(String parentId, String name, Set<String> ignored) {
        // TODO Auto-generated method stub
        // return null;
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasChild(String parentId, String name, Set<String> ignored) {
        // TODO Auto-generated method stub
        // return false;
        throw new UnsupportedOperationException();
    }

    @Override
    public List<State> queryKeyValue(String key, Object value, Set<String> ignored) {
        // TODO Auto-generated method stub
        // return null;
        throw new UnsupportedOperationException();
    }

    @Override
    public List<State> queryKeyValue(String key1, Object value1, String key2, Object value2, Set<String> ignored) {
        // TODO Auto-generated method stub
        // return null;
        throw new UnsupportedOperationException();
    }

    @Override
    public Stream<State> getDescendants(String id, Set<String> keys) {
        return getDescendants(id, keys, 0);
    }

    @Override
    public Stream<State> getDescendants(String id, Set<String> keys, int limit) {
        List<State> states = new ArrayList<>();
        try {
            String sql = "SELECT ";
            if (keys.isEmpty()) {
                sql += ID_COL;
            } else {
                throw new UnsupportedOperationException("TODO XXX");
            }
            sql += " FROM " + TABLE_NAME + " WHERE " + JSON_COL + "->'" + DBSDocument.KEY_ANCESTOR_IDS
                    + "' <@ ?::jsonb";
            // TODO limit
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                String jsonId = converter.serializableToJson(id);
                ps.setString(1, jsonId);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String did = rs.getString(1);
                        State state = new State();
                        state.put(KEY_ID, did);
                        states.add(state);
                    }
                }
            }
            return StreamSupport.stream(states.spliterator(), false);
        } catch (SQLException e) {
            throw new NuxeoException(e);
        }
    }

    @Override
    public boolean queryKeyValuePresence(String key, String value, Set<String> ignored) {
        // TODO Auto-generated method stub
        // return false;
        throw new UnsupportedOperationException();
    }

    @Override
    public PartialList<Map<String, Serializable>> queryAndFetch(DBSExpressionEvaluator evaluator,
            OrderByClause orderByClause, boolean distinctDocuments, int limit, int offset, int countUpTo) {
        // TODO Auto-generated method stub
        return new PartialList<>(Collections.emptyList(), 0);
    }

    @Override
    public ScrollResult<String> scroll(DBSExpressionEvaluator evaluator, int batchSize, int keepAliveSeconds) {
        // TODO Auto-generated method stub
        // return null;
        throw new UnsupportedOperationException();
    }

    @Override
    public ScrollResult<String> scroll(String scrollId) {
        // TODO Auto-generated method stub
        // return null;
        throw new UnsupportedOperationException();
    }

    @Override
    protected void initBlobsPaths() {
        // TODO Auto-generated method stub
    }

    @Override
    public void markReferencedBinaries() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

    @Override
    public Lock getLock(String id) {
        // TODO Auto-generated method stub
        // return null;
        throw new UnsupportedOperationException();
    }

    @Override
    public Lock setLock(String id, Lock lock) {
        // TODO Auto-generated method stub
        // return null;
        throw new UnsupportedOperationException();
    }

    @Override
    public Lock removeLock(String id, String owner) {
        // TODO Auto-generated method stub
        // return null;
        throw new UnsupportedOperationException();
    }

    @Override
    public void closeLockManager() {
        // TODO Auto-generated method stub
        //
        throw new UnsupportedOperationException();
    }

    @Override
    public void clearLockManagerCaches() {
        // TODO Auto-generated method stub
        //
        throw new UnsupportedOperationException();
    }

}
