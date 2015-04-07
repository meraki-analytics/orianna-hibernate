package com.robrua.orianna.store;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.hibernate.Criteria;
import org.hibernate.ScrollMode;
import org.hibernate.ScrollableResults;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.criterion.Restrictions;

import com.robrua.orianna.type.core.OriannaObject;
import com.robrua.orianna.type.dto.OriannaDto;
import com.robrua.orianna.type.exception.OriannaException;

/**
 * Provides Hibernate support naturally in the Orianna library. Use it just like
 * any other DataStore in the API, though you may have to take a look at <a
 * href="http://hibernate.org/">Hibernate</a> itself if you can't figure out how
 * to initialize it this.
 *
 * @author Rob Rua (robrua@alumni.cmu.edu)
 */
public class HibernateDB extends DataStore implements AutoCloseable {
    public static class Builder {
        private String autoSchema = "update";
        private String cacheProvider = "none";
        private String dialect = "org.hibernate.dialect.MySQLDialect";
        private String driver = "com.mysql.jdbc.Driver";
        private int entityClearTheshold = 100;
        private String password = null;
        private boolean showSQL = false;
        private String url = null;
        private String username = null;

        /**
         *
         */
        private Builder() {}

        /**
         * @param autoSchema
         *            hibernate.hbm2ddl.auto (default = update)
         * @return the builder
         */
        public Builder autoSchema(final String autoSchema) {
            this.autoSchema = autoSchema;
            return this;
        }

        /**
         * @return the datastore
         */
        public HibernateDB build() {
            if(url == null || username == null || password == null) {
                throw new IllegalArgumentException("URL, Username, and Password must be set!");
            }

            final Configuration configuration = new Configuration();

            // DB configuration
            configuration.setProperty("hibernate.dialect", dialect).setProperty("hibernate.connection.driver_class", driver)
                    .setProperty("hibernate.connection.url", url).setProperty("hibernate.connection.username", username)
                    .setProperty("hibernate.connection.password", password)

                    // Hibernate options
                    .setProperty("hibernate.cache.provider_class", cacheProvider).setProperty("hibernate.show_sql", Boolean.toString(showSQL))
                    .setProperty("hibernate.hbm2ddl.auto", autoSchema);

            return new HibernateDB(configuration, entityClearTheshold);
        }

        /**
         *
         * @param cacheProvider
         *            hibernate.cache.provider_class (default = none)
         * @return the builder
         */
        public Builder cacheProvider(final String cacheProvider) {
            this.cacheProvider = cacheProvider;
            return this;
        }

        /**
         * @param dialect
         *            hibernate.dialect (default =
         *            org.hibernate.dialect.MySQLDialect)
         * @return the builder
         */
        public Builder dialect(final String dialect) {
            this.dialect = dialect;
            return this;
        }

        /**
         * @param driver
         *            hibernate.connection.driver_class (default =
         *            com.mysql.jdbc.Driver)
         * @return the builder
         */
        public Builder driver(final String driver) {
            this.driver = driver;
            return this;
        }

        /**
         * Sets the maximum number of entities that will be managed by a single
         * hibernate session before clearing the session
         *
         * @param entityClearTheshold
         *            the maximum number of entities to manage
         * @return the builder
         */
        public Builder entityClearThreshold(final int entityClearTheshold) {
            this.entityClearTheshold = entityClearTheshold;
            return this;
        }

        /**
         * @param password
         *            hibernate.connection.password
         * @return the builder
         */
        public Builder password(final String password) {
            this.password = password;
            return this;
        }

        /**
         * @param showSQL
         *            hibernate.show_sql (default = false)
         * @return the builder
         */
        public Builder showSQL(final boolean showSQL) {
            this.showSQL = showSQL;
            return this;
        }

        /**
         * @param url
         *            hibernate.connection.url
         * @return the builder
         */
        public Builder URL(final String url) {
            this.url = url;
            return this;
        }

        /**
         * @param username
         *            hibernate.connection.username
         * @return the builder
         */
        public Builder username(final String username) {
            this.username = username;
            return this;
        }
    }

    private static class DBIterator<T extends OriannaObject<?>> extends CloseableIterator<T> {
        private final ScrollableResults result;
        private final Class<T> type;

        /**
         * @param type
         *            the type to iterate
         * @param result
         *            the results of the query
         */
        public DBIterator(final Class<T> type, final ScrollableResults result) {
            this.result = result;
            this.type = type;
        }

        @Override
        public void close() {
            result.close();
        }

        @Override
        public boolean hasNext() {
            return !result.isLast();
        }

        @SuppressWarnings("unchecked")
        @Override
        public T next() {
            result.next();
            try {
                return (T)type.getDeclaredConstructors()[0].newInstance(result.get(0));
            }
            catch(InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | SecurityException e) {
                throw new OriannaException("Couldn't load data from DB");
            }
        }
    }

    private static final long CHECK_MILLIS = 10000L;

    /**
     * @return a builder for a HibernateDB
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * @param clazz
     *            an OriannaObject class
     * @return the class of the underlying dto
     */
    @SuppressWarnings("unchecked")
    private static Class<? extends OriannaDto> getDtoClass(final Class<? extends OriannaObject<?>> clazz) {
        return (Class<? extends OriannaDto>)((ParameterizedType)clazz.getGenericSuperclass()).getActualTypeArguments()[0];
    }

    /**
     * @param clazz
     *            the dto type
     * @param key
     *            the key type
     * @return the name of the DB column that the key restriction is for
     */
    private static String getIndexRow(final Class<? extends OriannaDto> clazz, final Object key) {
        try {
            return clazz.newInstance().getDataStoreIndexField(key.getClass());
        }
        catch(InstantiationException | IllegalAccessException e) {
            throw new OriannaException("Couldn't instantiate dto");
        }
    }

    private final int entityClearTheshold;
    private final SessionManager sessionManager;

    /**
     * Initializes the database for a given hibernate configuration. Handles
     * adding the annotated dto classes into the config.
     *
     * @param cfg
     *            the Hibernate config to use for this DB
     * @param entityClearTheshold
     *            the maximum number of entities to manage
     */
    public HibernateDB(final Configuration cfg, final int entityClearTheshold) {
        this.entityClearTheshold = entityClearTheshold;
        
        // Add DTO classes
        cfg.addAnnotatedClass(com.robrua.orianna.type.dto.champion.Champion.class).addAnnotatedClass(com.robrua.orianna.type.dto.champion.ChampionList.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.currentgame.BannedChampion.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.currentgame.CurrentGameInfo.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.currentgame.Mastery.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.currentgame.Observer.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.currentgame.Participant.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.currentgame.Rune.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.featuredgames.FeaturedGames.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.game.Game.class).addAnnotatedClass(com.robrua.orianna.type.dto.game.Player.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.game.RawStats.class).addAnnotatedClass(com.robrua.orianna.type.dto.game.RecentGames.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.league.League.class).addAnnotatedClass(com.robrua.orianna.type.dto.league.LeagueEntry.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.league.MiniSeries.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.match.BannedChampion.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.match.CombinedParticipant.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.match.Event.class).addAnnotatedClass(com.robrua.orianna.type.dto.match.Frame.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.match.Mastery.class).addAnnotatedClass(com.robrua.orianna.type.dto.match.MatchDetail.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.match.Participant.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.match.ParticipantFrame.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.match.ParticipantIdentity.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.match.ParticipantStats.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.match.ParticipantTimeline.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.match.ParticipantTimelineData.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.match.Player.class).addAnnotatedClass(com.robrua.orianna.type.dto.match.Position.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.match.Rune.class).addAnnotatedClass(com.robrua.orianna.type.dto.match.Team.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.match.Timeline.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.matchhistory.MatchSummary.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.matchhistory.PlayerHistory.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.staticdata.BasicData.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.staticdata.BasicDataStats.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.staticdata.Block.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.staticdata.BlockItem.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.staticdata.Champion.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.staticdata.ChampionList.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.staticdata.ChampionSpell.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.staticdata.EffectList.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.staticdata.Gold.class).addAnnotatedClass(com.robrua.orianna.type.dto.staticdata.Group.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.staticdata.Image.class).addAnnotatedClass(com.robrua.orianna.type.dto.staticdata.Info.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.staticdata.Item.class).addAnnotatedClass(com.robrua.orianna.type.dto.staticdata.ItemList.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.staticdata.ItemTree.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.staticdata.LanguageStrings.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.staticdata.LevelTip.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.staticdata.MapData.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.staticdata.MapDetails.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.staticdata.Mastery.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.staticdata.MasteryList.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.staticdata.MasteryTree.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.staticdata.MasteryTreeItem.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.staticdata.MasteryTreeList.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.staticdata.MetaData.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.staticdata.Passive.class).addAnnotatedClass(com.robrua.orianna.type.dto.staticdata.Realm.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.staticdata.Recommended.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.staticdata.Rune.class).addAnnotatedClass(com.robrua.orianna.type.dto.staticdata.RuneList.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.staticdata.Skin.class).addAnnotatedClass(com.robrua.orianna.type.dto.staticdata.SpellVars.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.staticdata.Stats.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.staticdata.SummonerSpell.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.staticdata.SummonerSpellList.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.stats.AggregatedStats.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.stats.ChampionStats.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.stats.PlayerStatsSummary.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.stats.PlayerStatsSummaryList.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.stats.RankedStats.class).addAnnotatedClass(com.robrua.orianna.type.dto.status.Incident.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.status.Message.class).addAnnotatedClass(com.robrua.orianna.type.dto.status.Service.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.status.Shard.class).addAnnotatedClass(com.robrua.orianna.type.dto.status.ShardStatus.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.status.Translation.class).addAnnotatedClass(com.robrua.orianna.type.dto.summoner.Mastery.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.summoner.MasteryPage.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.summoner.MasteryPages.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.summoner.RunePage.class).addAnnotatedClass(com.robrua.orianna.type.dto.summoner.RunePages.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.summoner.RuneSlot.class).addAnnotatedClass(com.robrua.orianna.type.dto.summoner.Summoner.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.team.MatchHistorySummary.class).addAnnotatedClass(com.robrua.orianna.type.dto.team.Roster.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.team.Team.class).addAnnotatedClass(com.robrua.orianna.type.dto.team.TeamMemberInfo.class)
        .addAnnotatedClass(com.robrua.orianna.type.dto.team.TeamStatDetail.class).addAnnotatedClass(com.robrua.orianna.store.HasAllStatus.class);

        final StandardServiceRegistryBuilder ssrb = new StandardServiceRegistryBuilder().applySettings(cfg.getProperties());
        sessionManager = new SessionManager(cfg.buildSessionFactory(ssrb.build()), CHECK_MILLIS);
    }

    @Override
    protected boolean allowsNullStoreKeys() {
        return true;
    }

    @Override
    protected <T extends OriannaObject<?>> boolean checkHasAll(final Class<T> type) {
        final HasAllStatus status = hibernateGet(HasAllStatus.class, "clazz", type);
        if(status == null) {
            return false;
        }

        return status.isHasAll();
    }

    @Override
    public void close() {
        sessionManager.close();
    }

    @Override
    protected <T extends OriannaObject<?>> void doDelete(final Class<T> type, final List<?> keys) {
        final Class<? extends OriannaDto> clazz = getDtoClass(type);

        final String indexRow = getIndexRow(clazz, keys.get(0));
        if(indexRow == null) {
            if(keys.get(0).getClass().equals(Long.class)) {
                throw new OriannaException("Invalid key type. Bear with me, this can be finicky. Try using an Integer instead.");
            }
            else if(keys.get(0).getClass().equals(Integer.class)) {
                throw new OriannaException("Invalid key type. Bear with me, this can be finicky. Try using a Long instead.");
            }
            else {
                throw new OriannaException("Invalid key type");
            }
        }

        final Set<OriannaDto> result = new HashSet<>();
        for(final Object key : keys) {
            final OriannaDto res = hibernateGet(clazz, indexRow, key);
            if(res != null) {
                result.add(res);
            }
        }
        hibernateDeleteAll(result);
    }

    @Override
    protected <T extends OriannaObject<?>> void doDelete(final Class<T> type, final Object key) {
        final Class<? extends OriannaDto> clazz = getDtoClass(type);

        final String indexRow = getIndexRow(clazz, key);
        if(indexRow == null) {
            if(key.getClass().equals(Long.class)) {
                throw new OriannaException("Invalid key type. Bear with me, this can be finicky. Try using an Integer instead.");
            }
            else if(key.getClass().equals(Integer.class)) {
                throw new OriannaException("Invalid key type. Bear with me, this can be finicky. Try using a Long instead.");
            }
            else {
                throw new OriannaException("Invalid key type");
            }
        }

        final OriannaDto result = hibernateGet(clazz, indexRow, key);
        if(result != null) {
            hibernateDelete(result);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    protected <T extends OriannaObject<?>> List<T> doGet(final Class<T> type, final List<?> keys) {
        final Class<? extends OriannaDto> clazz = getDtoClass(type);

        final String indexRow = getIndexRow(clazz, keys.get(0));
        if(indexRow == null) {
            if(keys.get(0).getClass().equals(Long.class)) {
                throw new OriannaException("Invalid key type. Bear with me, this can be finicky. Try using an Integer instead.");
            }
            else if(keys.get(0).getClass().equals(Integer.class)) {
                throw new OriannaException("Invalid key type. Bear with me, this can be finicky. Try using a Long instead.");
            }
            else {
                throw new OriannaException("Invalid key type");
            }
        }

        try {
            final List<T> result = new ArrayList<>();
            for(final Object key : keys) {
                final OriannaDto res = hibernateGet(clazz, indexRow, key);
                if(res == null) {
                    result.add(null);
                }
                else {
                    result.add((T)type.getDeclaredConstructors()[0].newInstance(res));
                }
            }
            return result;
        }
        catch(InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | SecurityException e) {
            throw new OriannaException("Couldn't load data from DB");
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    protected <T extends OriannaObject<?>> T doGet(final Class<T> type, final Object key) {
        final Class<? extends OriannaDto> clazz = getDtoClass(type);

        final String indexRow = getIndexRow(clazz, key);
        if(indexRow == null) {
            if(key.getClass().equals(Long.class)) {
                throw new OriannaException("Invalid key type. Bear with me, this can be finicky. Try using an Integer instead.");
            }
            else if(key.getClass().equals(Integer.class)) {
                throw new OriannaException("Invalid key type. Bear with me, this can be finicky. Try using a Long instead.");
            }
            else {
                throw new OriannaException("Invalid key type");
            }
        }

        final OriannaDto result = hibernateGet(clazz, indexRow, key);
        if(result == null) {
            return null;
        }

        try {
            return (T)type.getDeclaredConstructors()[0].newInstance(result);
        }
        catch(InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | SecurityException e) {
            throw new OriannaException("Couldn't load data from DB");
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    protected <T extends OriannaObject<?>> List<T> doGetAll(final Class<T> type) {
        final Class<? extends OriannaDto> clazz = getDtoClass(type);

        final ScrollableResults result = hibernateGetAll(clazz);
        if(result == null) {
            return Collections.emptyList();
        }

        try {
            final List<T> response = new ArrayList<>();
            while(result.next()) {
                response.add((T)type.getDeclaredConstructors()[0].newInstance(result.get(0)));
            }
            result.close();
            return response;
        }
        catch(InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | SecurityException e) {
            throw new OriannaException("Couldn't load data from DB");
        }
    }

    @Override
    protected <T extends OriannaObject<?>> CloseableIterator<T> doGetIterator(final Class<T> type) {
        final Class<? extends OriannaDto> clazz = getDtoClass(type);

        final ScrollableResults result = hibernateGetAll(clazz);
        if(result == null) {
            return CloseableIterator.emptyIterator();
        }

        return new DBIterator<>(type, result);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected <T extends OriannaObject<?>> void doStore(final List<T> objs, final List<?> keys, final boolean isFullSet) {
        final Class<T> type = (Class<T>)objs.get(0).getClass();

        final Set<Object> items = new HashSet<>();
        if(isFullSet) {
            final HasAllStatus status = new HasAllStatus();
            status.setClazz(type);
            status.setHasAll(true);
            items.add(status);
        }

        for(final T obj : objs) {
            items.add(obj.getDto());
        }

        hibernateSaveAll(items);
    }

    @Override
    protected <T extends OriannaObject<?>> void doStore(final T obj, final Object key) {
        hibernateSave(obj.getDto());
    }

    /**
     * @param obj
     *            the object to delete
     */
    private void hibernateDelete(final Object obj) {
        final Session session = sessionManager.getSession();
        final Transaction tx = session.beginTransaction();
        session.delete(obj);
        tx.commit();
    }

    /**
     * @param objs
     *            the objects to delete
     */
    private void hibernateDeleteAll(final Collection<?> objs) {
        final Session session = sessionManager.getSession();
        final Transaction tx = session.beginTransaction();
        for(final Object obj : objs) {
            session.delete(obj);
        }
        tx.commit();
    }

    /**
     * @param clazz
     *            the class to get
     * @param searchField
     *            the column to search on
     * @param searchVal
     *            the value to search for
     * @return the object meeting the criteria, or null
     */
    @SuppressWarnings("unchecked")
    private <T> T hibernateGet(final Class<T> clazz, final String searchField, final Object searchVal) {
        final Session session = sessionManager.getSession();
        final Criteria queryCriteria = session.createCriteria(clazz);
        queryCriteria.add(Restrictions.eq(searchField, searchVal));
        return (T)queryCriteria.uniqueResult();
    }

    /**
     * @param clazz
     *            the class to iterate over
     * @return the hibernate results for that class
     */
    private ScrollableResults hibernateGetAll(final Class<?> clazz) {
        final Session session = sessionManager.getSession();
        return session.createCriteria(clazz).scroll(ScrollMode.FORWARD_ONLY);
    }

    /**
     * @param obj
     *            the object to save
     */
    private void hibernateSave(final Object obj) {
        final Session session = sessionManager.getSession();
        final Transaction tx = session.beginTransaction();
        session.merge(obj);

        if(session.getStatistics().getEntityCount() >= entityClearTheshold) {
            session.flush();
            session.clear();
        }
        tx.commit();
    }

    /**
     * @param objs
     *            the objects to save
     */
    private void hibernateSaveAll(final Collection<?> objs) {
        final Session session = sessionManager.getSession();
        final Transaction tx = session.beginTransaction();
        for(final Object obj : objs) {
            session.merge(obj);

            if(session.getStatistics().getEntityCount() >= entityClearTheshold) {
                session.flush();
                session.clear();
            }
        }
        tx.commit();
    }
}
