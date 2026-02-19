<?xml version="1.0" encoding="UTF-8"?>
<?eclipse version="3.2"?>
<plugin id="org.datanucleus" name="DataNucleus Core" provider-name="DataNucleus">
  <!-- extension points from datanucleus-core-4.1.6 -->
  <extension-point id="api_adapter" name="Api Adapter" schema="schema/apiadapter.exsd"/>
  <extension-point id="autostart" name="AutoStartMechanism" schema="schema/autostart.exsd"/>
  <extension-point id="cache_level1" name="Level1 Cache" schema="schema/cache_level1.exsd"/>
  <extension-point id="cache_level2" name="Level2 Cache" schema="schema/cache_level2.exsd"/>
  <extension-point id="cache_query_compilation" name="Query Compilation Cache" schema="schema/cache_query_compilation.exsd"/>
  <extension-point id="cache_query_compilation_store" name="Query Datastore Compilation Cache" schema="schema/cache_query_compilation_store.exsd"/>
  <extension-point id="cache_query_result" name="Query Result Cache" schema="schema/cache_query_result.exsd"/>
  <extension-point id="callbackhandler" name="Callback Handler" schema="schema/callbackhandler.exsd"/>
  <extension-point id="classloader_resolver" name="ClassLoader Resolver" schema="schema/classloader_resolver.exsd"/>
  <extension-point id="identity_string_translator" name="Identity String Translator" schema="schema/identity_string_translator.exsd"/>
  <extension-point id="identity_key_translator" name="Identity Key Translator" schema="schema/identity_key_translator.exsd"/>
  <extension-point id="jta_locator" name="JTA Locator" schema="schema/jta_locator.exsd"/>
  <extension-point id="management_server" name="Management Server" schema="schema/management_server.exsd"/>
  <extension-point id="store_datastoreidentity" name="Datastore Identity" schema="schema/datastoreidentity.exsd"/>
  <extension-point id="store_manager" name="Store Manager" schema="schema/store_manager.exsd"/>
  <extension-point id="store_query_query" name="Query" schema="schema/query.exsd"/>
  <extension-point id="query_method_evaluators" name="Query Method Evaluators" schema="schema/query_method_evaluator.exsd"/>
  <extension-point id="query_method_prefix" name="Query Method Prefix" schema="schema/query_method_prefix.exsd"/>
  <extension-point id="store_valuegenerator" name="Value Generator" schema="schema/valuegenerator.exsd"/>
  <extension-point id="store_objectvaluegenerator" name="Object Value Generator" schema="schema/objectvaluegenerator.exsd"/>
  <extension-point id="store_connectionfactory" name="Connection Factory" schema="schema/connectionfactory.exsd"/>
  <extension-point id="java_type" name="Java Type" schema="schema/java_type.exsd"/>
  <extension-point id="type_converter" name="Java Converter Type" schema="schema/type_converter.exsd"/>
  <extension-point id="persistence_properties" name="Persistence Properties" schema="schema/persistenceproperties.exsd"/>
  <extension-point id="metadata_handler" name="MetaData Handler" schema="schema/metadata_handler.exsd"/>
  <extension-point id="metadata_manager" name="MetaDataManager" schema="schema/metadata_manager.exsd"/>
  <extension-point id="metadata_entityresolver" name="MetaDataEntityResolver" schema="schema/metadata_entityresolver.exsd"/>
  <extension-point id="annotations" name="Annotations" schema="schema/annotations.exsd"/>
  <extension-point id="class_annotation_handler" name="Class Annotation Handler" schema="schema/class_annotation_handler.exsd"/>
  <extension-point id="member_annotation_handler" name="Member Annotation Handler" schema="schema/member_annotation_handler.exsd"/>
  <extension-point id="identifier_namingfactory" name="Identifier NamingFactory" schema="schema/identifier_namingfactory.exsd"/>

  <!-- extension-points from datanucleus-rdbms-4.17 -->
  <extension-point id="store.rdbms.datastoreadapter" name="Datastore Adapter" schema="schema/datastoreadapter.exsd"/>
  <extension-point id="store.rdbms.connectionprovider" name="Connection Provider" schema="schema/connectionprovider.exsd"/>
  <extension-point id="store.rdbms.connectionpool" name="ConnectionPool" schema="schema/connectionpool.exsd"/>
  <extension-point id="store.rdbms.sql_expression" name="SQL Expressions" schema="schema/sql_expression.exsd"/>
  <extension-point id="store.rdbms.sql_method" name="SQL Methods" schema="schema/sql_method.exsd"/>
  <extension-point id="store.rdbms.sql_operation" name="SQL Expressions" schema="schema/sql_operation.exsd"/>
  <extension-point id="store.rdbms.sql_tablenamer" name="SQL Table Namer" schema="schema/sql_tablenamer.exsd"/>
  <extension-point id="store.rdbms.java_mapping" name="Types Mapping" schema="schema/java_mapping.exsd"/>
  <extension-point id="store.rdbms.datastore_mapping" name="Datastore Mapping" schema="schema/datastore_mapping.exsd"/>
  <extension-point id="store.rdbms.identifierfactory" name="Identifier Factory" schema="schema/identifierfactory.exsd"/>

  <!-- extensions from datanucleus-core-4.1.6 -->
  <!-- LEVEL1 CACHES -->
  <extension point="org.datanucleus.cache_level1">
    <cache name="strong" class-name="org.datanucleus.cache.StrongRefCache"/>
    <cache name="soft" class-name="org.datanucleus.cache.SoftRefCache"/>
    <cache name="weak" class-name="org.datanucleus.cache.WeakRefCache"/>
  </extension>

  <!-- LEVEL2 CACHES -->
  <extension point="org.datanucleus.cache_level2">
    <cache name="weak" class-name="org.datanucleus.cache.WeakLevel2Cache"/>
    <cache name="soft" class-name="org.datanucleus.cache.SoftLevel2Cache"/>
    <cache name="none" class-name="org.datanucleus.cache.NullLevel2Cache"/>
    <cache name="javax.cache" class-name="org.datanucleus.cache.JavaxCacheLevel2Cache"/>
  </extension>

  <!-- JTA LOCATORS -->
  <extension point="org.datanucleus.jta_locator">
    <jta_locator name="jboss" class-name="org.datanucleus.transaction.jta.JBossTransactionManagerLocator"/>
    <jta_locator name="jonas" class-name="org.datanucleus.transaction.jta.JOnASTransactionManagerLocator"/>
    <jta_locator name="jotm" class-name="org.datanucleus.transaction.jta.JOTMTransactionManagerLocator"/>
    <jta_locator name="oc4j" class-name="org.datanucleus.transaction.jta.OC4JTransactionManagerLocator"/>
    <jta_locator name="orion" class-name="org.datanucleus.transaction.jta.OrionTransactionManagerLocator"/>
    <jta_locator name="resin" class-name="org.datanucleus.transaction.jta.ResinTransactionManagerLocator"/>
    <jta_locator name="sap" class-name="org.datanucleus.transaction.jta.SAPWebASTransactionManagerLocator"/>
    <jta_locator name="sun" class-name="org.datanucleus.transaction.jta.SunTransactionManagerLocator"/>
    <jta_locator name="weblogic" class-name="org.datanucleus.transaction.jta.WebLogicTransactionManagerLocator"/>
    <jta_locator name="websphere" class-name="org.datanucleus.transaction.jta.WebSphereTransactionManagerLocator"/>
    <jta_locator name="custom_jndi" class-name="org.datanucleus.transaction.jta.CustomJNDITransactionManagerLocator"/>
    <jta_locator name="atomikos" class-name="org.datanucleus.transaction.jta.AtomikosTransactionManagerLocator"/>
    <jta_locator name="bitronix" class-name="org.datanucleus.transaction.jta.BTMTransactionManagerLocator"/>
  </extension>

  <!-- DATASTORE IDENTITY -->
  <extension point="org.datanucleus.store_datastoreidentity">
    <datastoreidentity name="datanucleus" class-name="org.datanucleus.identity.DatastoreIdImpl"/>
    <datastoreidentity name="kodo" class-name="org.datanucleus.identity.DatastoreIdImplKodo"/>
    <datastoreidentity name="xcalia" class-name="org.datanucleus.identity.DatastoreIdImplXcalia"/>
    <datastoreidentity name="unique" class-name="org.datanucleus.identity.DatastoreUniqueLongId"/>
  </extension>

  <!-- IDENTITY STRING TRANSLATOR -->
  <extension point="org.datanucleus.identity_string_translator">
    <identitystringtranslator name="xcalia" class-name="org.datanucleus.identity.XcaliaIdentityStringTranslator"/>
  </extension>

  <!-- NAMING FACTORY -->
  <extension point="org.datanucleus.identifier_namingfactory">
    <namingfactory name="datanucleus2" class-name="org.datanucleus.store.schema.naming.DN2NamingFactory"/>
    <namingfactory name="jpa" class-name="org.datanucleus.store.schema.naming.JPANamingFactory"/>
  </extension>

  <!-- JAVA TYPES -->
  <extension point="org.datanucleus.java_type">
    <!-- "primitive" types -->
    <java-type name="boolean" dfg="true" embedded="true"/>
    <java-type name="byte" dfg="true" embedded="true"/>
    <java-type name="char" dfg="true" embedded="true"/>
    <java-type name="double" dfg="true" embedded="true"/>
    <java-type name="float" dfg="true" embedded="true"/>
    <java-type name="int" dfg="true" embedded="true"/>
    <java-type name="long" dfg="true" embedded="true"/>
    <java-type name="short" dfg="true" embedded="true"/>

    <!-- "java.lang" types -->
    <java-type name="java.lang.Boolean" dfg="true" embedded="true"/>
    <java-type name="java.lang.Byte" dfg="true" embedded="true"/>
    <java-type name="java.lang.Character" dfg="true" embedded="true"/>
    <java-type name="java.lang.Double" dfg="true" embedded="true"/>
    <java-type name="java.lang.Float" dfg="true" embedded="true"/>
    <java-type name="java.lang.Integer" dfg="true" embedded="true"/>
    <java-type name="java.lang.Long" dfg="true" embedded="true"/>
    <java-type name="java.lang.Short" dfg="true" embedded="true"/>
    <java-type name="java.lang.Number" dfg="true" embedded="true"/>
    <java-type name="java.lang.String" dfg="true" embedded="true"/>
    <java-type name="java.lang.Enum" dfg="true" embedded="true"/>
    <java-type name="java.lang.StringBuffer" dfg="true" embedded="true" converter-name="dn.stringbuffer-string"/>
    <java-type name="java.lang.StringBuilder" dfg="true" embedded="true" converter-name="dn.stringbuilder-string"/>
    <java-type name="java.lang.Class" dfg="true" embedded="true" converter-name="dn.class-string"/>

    <!-- "java.awt" types -->
    <java-type name="java.awt.image.BufferedImage" embedded="true"/>
    <java-type name="java.awt.Color" embedded="true" converter-name="dn.color-string"/>

    <!-- "java.math" types -->
    <java-type name="java.math.BigDecimal" dfg="true" embedded="true"/>
    <java-type name="java.math.BigInteger" dfg="true" embedded="true"/>

    <!-- "java.net" types -->
    <java-type name="java.net.URL" dfg="true" embedded="true" converter-name="dn.url-string"/>
    <java-type name="java.net.URI" dfg="true" embedded="true" converter-name="dn.uri-string"/>

    <!-- "java.sql" types -->
    <java-type name="java.sql.Date" dfg="true" embedded="true" wrapper-type="org.datanucleus.store.types.wrappers.SqlDate"/>
    <java-type name="java.sql.Time" dfg="true" embedded="true" wrapper-type="org.datanucleus.store.types.wrappers.SqlTime"/>
    <java-type name="java.sql.Timestamp" dfg="true" embedded="true" wrapper-type="org.datanucleus.store.types.wrappers.SqlTimestamp"/>

    <!-- "java.util" types -->
    <java-type name="java.util.Date" dfg="true" embedded="true" wrapper-type="org.datanucleus.store.types.wrappers.Date"/>
    <java-type name="java.util.Locale" dfg="true" embedded="true" converter-name="dn.locale-string"/>
    <java-type name="java.util.Currency" dfg="true" embedded="true" converter-name="dn.currency-string"/>
    <java-type name="java.util.Calendar" dfg="true" embedded="true"
               wrapper-type="org.datanucleus.store.types.wrappers.GregorianCalendar" converter-name="dn.calendar-string"/>
    <java-type name="java.util.GregorianCalendar" dfg="true" embedded="true"
               wrapper-type="org.datanucleus.store.types.wrappers.GregorianCalendar" converter-name="dn.calendar-string"/>
    <java-type name="java.util.UUID" dfg="true" embedded="true" converter-name="dn.uuid-string"/>
    <java-type name="java.util.TimeZone" dfg="true" embedded="true" converter-name="dn.timezone-string"/>

    <java-type name="java.util.ArrayList" wrapper-type="org.datanucleus.store.types.wrappers.ArrayList"
               wrapper-type-backed="org.datanucleus.store.types.wrappers.backed.ArrayList"/>
    <java-type name="java.util.Arrays$ArrayList" wrapper-type="org.datanucleus.store.types.wrappers.List"
               wrapper-type-backed="org.datanucleus.store.types.wrappers.backed.List"/>
    <java-type name="java.util.BitSet" wrapper-type="org.datanucleus.store.types.wrappers.BitSet" converter-name="dn.bitset-string"/>
    <java-type name="java.util.Collection" wrapper-type="org.datanucleus.store.types.wrappers.Collection"
               wrapper-type-backed="org.datanucleus.store.types.wrappers.backed.Collection"/>
    <java-type name="java.util.HashMap" wrapper-type="org.datanucleus.store.types.wrappers.HashMap"
               wrapper-type-backed="org.datanucleus.store.types.wrappers.backed.HashMap"/>
    <java-type name="java.util.HashSet" wrapper-type="org.datanucleus.store.types.wrappers.HashSet"
               wrapper-type-backed="org.datanucleus.store.types.wrappers.backed.HashSet"/>
    <java-type name="java.util.Hashtable" wrapper-type="org.datanucleus.store.types.wrappers.Hashtable"
               wrapper-type-backed="org.datanucleus.store.types.wrappers.backed.Hashtable"/>
    <java-type name="java.util.LinkedHashMap" wrapper-type="org.datanucleus.store.types.wrappers.LinkedHashMap"
               wrapper-type-backed="org.datanucleus.store.types.wrappers.backed.LinkedHashMap"/>
    <java-type name="java.util.LinkedHashSet" wrapper-type="org.datanucleus.store.types.wrappers.LinkedHashSet"
               wrapper-type-backed="org.datanucleus.store.types.wrappers.backed.LinkedHashSet"/>
    <java-type name="java.util.LinkedList" wrapper-type="org.datanucleus.store.types.wrappers.LinkedList"
               wrapper-type-backed="org.datanucleus.store.types.wrappers.backed.LinkedList"/>
    <java-type name="java.util.List" wrapper-type="org.datanucleus.store.types.wrappers.List"
               wrapper-type-backed="org.datanucleus.store.types.wrappers.backed.List"/>
    <java-type name="java.util.Map" wrapper-type="org.datanucleus.store.types.wrappers.Map"
               wrapper-type-backed="org.datanucleus.store.types.wrappers.backed.Map"/>
    <java-type name="java.util.PriorityQueue" wrapper-type="org.datanucleus.store.types.wrappers.PriorityQueue"
               wrapper-type-backed="org.datanucleus.store.types.wrappers.backed.PriorityQueue"/>
    <java-type name="java.util.Properties" wrapper-type="org.datanucleus.store.types.wrappers.Properties"
               wrapper-type-backed="org.datanucleus.store.types.wrappers.backed.Properties"/>
    <java-type name="java.util.Queue" wrapper-type="org.datanucleus.store.types.wrappers.Queue"
               wrapper-type-backed="org.datanucleus.store.types.wrappers.backed.Queue"/>
    <java-type name="java.util.Set" wrapper-type="org.datanucleus.store.types.wrappers.Set"
               wrapper-type-backed="org.datanucleus.store.types.wrappers.backed.Set"/>
    <java-type name="java.util.SortedSet" wrapper-type="org.datanucleus.store.types.wrappers.SortedSet"
               wrapper-type-backed="org.datanucleus.store.types.wrappers.backed.SortedSet"/>
    <java-type name="java.util.SortedMap" wrapper-type="org.datanucleus.store.types.wrappers.SortedMap"
               wrapper-type-backed="org.datanucleus.store.types.wrappers.backed.SortedMap"/>
    <java-type name="java.util.Stack" wrapper-type="org.datanucleus.store.types.wrappers.Stack"
               wrapper-type-backed="org.datanucleus.store.types.wrappers.backed.Stack"/>
    <java-type name="java.util.TreeMap" wrapper-type="org.datanucleus.store.types.wrappers.TreeMap"
               wrapper-type-backed="org.datanucleus.store.types.wrappers.backed.TreeMap"/>
    <java-type name="java.util.TreeSet" wrapper-type="org.datanucleus.store.types.wrappers.TreeSet"
               wrapper-type-backed="org.datanucleus.store.types.wrappers.backed.TreeSet"/>
    <java-type name="java.util.Vector" wrapper-type="org.datanucleus.store.types.wrappers.Vector"
               wrapper-type-backed="org.datanucleus.store.types.wrappers.backed.Vector"/>

    <!-- array types -->
    <java-type name="[B" embedded="true"/>
    <java-type name="[C" embedded="true"/>
    <java-type name="[D" embedded="true"/>
    <java-type name="[F" embedded="true"/>
    <java-type name="[I" embedded="true"/>
    <java-type name="[J" embedded="true"/>
    <java-type name="[S" embedded="true"/>
    <java-type name="[Z" embedded="true"/>
    <java-type name="[Ljava.lang.Boolean;" embedded="true"/>
    <java-type name="[Ljava.lang.Byte;" embedded="true"/>
    <java-type name="[Ljava.lang.Character;" embedded="true"/>
    <java-type name="[Ljava.lang.Double;" embedded="true"/>
    <java-type name="[Ljava.lang.Float;" embedded="true"/>
    <java-type name="[Ljava.lang.Integer;" embedded="true"/>
    <java-type name="[Ljava.lang.Long;" embedded="true"/>
    <java-type name="[Ljava.lang.Short;" embedded="true"/>
    <java-type name="[Ljava.lang.Number;" embedded="true"/>
    <java-type name="[Ljava.lang.String;" embedded="true"/>
    <java-type name="[Ljava.math.BigInteger;" embedded="true"/>
    <java-type name="[Ljava.math.BigDecimal;" embedded="true"/>
    <java-type name="[Ljava.util.Date;" embedded="true"/>
    <java-type name="[Ljava.util.Locale;" embedded="true"/>
    <java-type name="[Ljava.lang.Enum;" embedded="true"/>
  </extension>

  <!-- TYPE CONVERTERS -->
  <extension point="org.datanucleus.type_converter">
    <type-converter name="dn.boolean-yn" member-type="java.lang.Boolean" datastore-type="java.lang.Character"
                    converter-class="org.datanucleus.store.types.converters.BooleanYNConverter"/>
    <type-converter name="dn.boolean-integer" member-type="java.lang.Boolean" datastore-type="java.lang.Integer"
                    converter-class="org.datanucleus.store.types.converters.BooleanIntegerConverter"/>

    <type-converter name="dn.character-string" member-type="java.lang.Character" datastore-type="java.lang.String"
                    converter-class="org.datanucleus.store.types.converters.CharacterStringConverter"/>
    <type-converter name="dn.bigdecimal-string" member-type="java.math.BigDecimal" datastore-type="java.lang.String"
                    converter-class="org.datanucleus.store.types.converters.BigDecimalStringConverter"/>
    <type-converter name="dn.bigdecimal-double" member-type="java.math.BigDecimal" datastore-type="java.lang.Double"
                    converter-class="org.datanucleus.store.types.converters.BigDecimalDoubleConverter"/>
    <type-converter name="dn.biginteger-string" member-type="java.math.BigInteger" datastore-type="java.lang.String"
                    converter-class="org.datanucleus.store.types.converters.BigIntegerStringConverter"/>
    <type-converter name="dn.biginteger-long" member-type="java.math.BigInteger" datastore-type="java.lang.Long"
                    converter-class="org.datanucleus.store.types.converters.BigIntegerLongConverter"/>
    <type-converter name="dn.bitset-string" member-type="java.util.BitSet" datastore-type="java.lang.String"
                    converter-class="org.datanucleus.store.types.converters.BitSetStringConverter"/>

    <type-converter name="dn.calendar-string" member-type="java.util.Calendar" datastore-type="java.lang.String"
                    converter-class="org.datanucleus.store.types.converters.CalendarStringConverter"/>
    <type-converter name="dn.calendar-date" member-type="java.util.Calendar" datastore-type="java.util.Date"
                    converter-class="org.datanucleus.store.types.converters.CalendarDateConverter"/>
    <type-converter name="dn.calendar-timestamp" member-type="java.util.Calendar" datastore-type="java.sql.Timestamp"
                    converter-class="org.datanucleus.store.types.converters.CalendarTimestampConverter"/>
    <type-converter name="dn.calendar-components" member-type="java.util.Calendar" datastore-type="[Ljava.lang.Object;"
                    converter-class="org.datanucleus.store.types.converters.CalendarComponentsConverter"/>

    <type-converter name="dn.color-string" member-type="java.awt.Color" datastore-type="java.lang.String"
                    converter-class="org.datanucleus.store.types.converters.ColorStringConverter"/>
    <type-converter name="dn.color-components" member-type="java.awt.Color" datastore-type="[I"
                    converter-class="org.datanucleus.store.types.converters.ColorComponentsConverter"/>

    <type-converter name="dn.class-string" member-type="java.lang.Class" datastore-type="java.lang.String"
                    converter-class="org.datanucleus.store.types.converters.ClassStringConverter"/>
    <type-converter name="dn.integer-string" member-type="java.lang.Integer" datastore-type="java.lang.String"
                    converter-class="org.datanucleus.store.types.converters.IntegerStringConverter"/>
    <type-converter name="dn.long-string" member-type="java.lang.Long" datastore-type="java.lang.String"
                    converter-class="org.datanucleus.store.types.converters.LongStringConverter"/>
    <type-converter name="dn.currency-string" member-type="java.util.Currency" datastore-type="java.lang.String"
                    converter-class="org.datanucleus.store.types.converters.CurrencyStringConverter"/>
    <type-converter name="dn.locale-string" member-type="java.util.Locale" datastore-type="java.lang.String"
                    converter-class="org.datanucleus.store.types.converters.LocaleStringConverter"/>
    <type-converter name="dn.stringbuffer-string" member-type="java.lang.StringBuffer" datastore-type="java.lang.String"
                    converter-class="org.datanucleus.store.types.converters.StringBufferStringConverter"/>
    <type-converter name="dn.stringbuilder-string" member-type="java.lang.StringBuilder" datastore-type="java.lang.String"
                    converter-class="org.datanucleus.store.types.converters.StringBuilderStringConverter"/>
    <type-converter name="dn.timezone-string" member-type="java.util.TimeZone" datastore-type="java.lang.String"
                    converter-class="org.datanucleus.store.types.converters.TimeZoneStringConverter"/>
    <type-converter name="dn.uri-string" member-type="java.net.URI" datastore-type="java.lang.String"
                    converter-class="org.datanucleus.store.types.converters.URIStringConverter"/>
    <type-converter name="dn.url-string" member-type="java.net.URL" datastore-type="java.lang.String"
                    converter-class="org.datanucleus.store.types.converters.URLStringConverter"/>
    <type-converter name="dn.uuid-string" member-type="java.util.UUID" datastore-type="java.lang.String"
                    converter-class="org.datanucleus.store.types.converters.UUIDStringConverter"/>

    <type-converter name="dn.date-long" member-type="java.util.Date" datastore-type="java.lang.Long"
                    converter-class="org.datanucleus.store.types.converters.DateLongConverter"/>
    <type-converter name="dn.date-string" member-type="java.util.Date" datastore-type="java.lang.String"
                    converter-class="org.datanucleus.store.types.converters.DateStringConverter"/>

    <type-converter name="dn.sqldate-long" member-type="java.sql.Date" datastore-type="java.lang.Long"
                    converter-class="org.datanucleus.store.types.converters.SqlDateLongConverter"/>
    <type-converter name="dn.sqldate-string" member-type="java.sql.Date" datastore-type="java.lang.String"
                    converter-class="org.datanucleus.store.types.converters.SqlDateStringConverter"/>
    <type-converter name="dn.sqldate-date" member-type="java.sql.Date" datastore-type="java.util.Date"
                    converter-class="org.datanucleus.store.types.converters.SqlDateDateConverter"/>
    <type-converter name="dn.sqltime-long" member-type="java.sql.Time" datastore-type="java.lang.Long"
                    converter-class="org.datanucleus.store.types.converters.SqlTimeLongConverter"/>
    <type-converter name="dn.sqltime-string" member-type="java.sql.Time" datastore-type="java.lang.String"
                    converter-class="org.datanucleus.store.types.converters.SqlTimeStringConverter"/>
    <type-converter name="dn.sqltime-date" member-type="java.sql.Time" datastore-type="java.util.Date"
                    converter-class="org.datanucleus.store.types.converters.SqlTimeDateConverter"/>
    <type-converter name="dn.sqltimestamp-long" member-type="java.sql.Timestamp" datastore-type="java.lang.Long"
                    converter-class="org.datanucleus.store.types.converters.SqlTimestampLongConverter"/>
    <type-converter name="dn.sqltimestamp-date" member-type="java.sql.Timestamp" datastore-type="java.util.Date"
                    converter-class="org.datanucleus.store.types.converters.SqlTimestampDateConverter"/>
    <type-converter name="dn.sqltimestamp-string" member-type="java.sql.Timestamp" datastore-type="java.lang.String"
                    converter-class="org.datanucleus.store.types.converters.SqlTimestampStringConverter"/>

    <type-converter name="dn.serializable-string" member-type="java.io.Serializable" datastore-type="java.lang.String"
                    converter-class="org.datanucleus.store.types.converters.SerializableStringConverter"/>
    <type-converter name="dn.serializable-bytearray" member-type="java.io.Serializable" datastore-type="[B"
                    converter-class="org.datanucleus.store.types.converters.SerializableByteArrayConverter"/>
    <type-converter name="dn.serializable-bytebuffer" member-type="java.io.Serializable" datastore-type="java.nio.ByteBuffer"
                    converter-class="org.datanucleus.store.types.converters.SerializableByteBufferConverter"/>

    <type-converter name="dn.bytearray-bytebuffer" member-type="[B" datastore-type="java.nio.ByteBuffer"
                    converter-class="org.datanucleus.store.types.converters.ByteArrayByteBufferConverter"/>
    <type-converter name="dn.booleanarray-bytebuffer" member-type="[Z" datastore-type="java.nio.ByteBuffer"
                    converter-class="org.datanucleus.store.types.converters.BooleanArrayByteBufferConverter"/>
    <type-converter name="dn.chararray-bytebuffer" member-type="[C" datastore-type="java.nio.ByteBuffer"
                    converter-class="org.datanucleus.store.types.converters.CharArrayByteBufferConverter"/>
    <type-converter name="dn.doublearray-bytebuffer" member-type="[D" datastore-type="java.nio.ByteBuffer"
                    converter-class="org.datanucleus.store.types.converters.DoubleArrayByteBufferConverter"/>
    <type-converter name="dn.floatarray-bytebuffer" member-type="[F" datastore-type="java.nio.ByteBuffer"
                    converter-class="org.datanucleus.store.types.converters.FloatArrayByteBufferConverter"/>
    <type-converter name="dn.intarray-bytebuffer" member-type="[I" datastore-type="java.nio.ByteBuffer"
                    converter-class="org.datanucleus.store.types.converters.IntArrayByteBufferConverter"/>
    <type-converter name="dn.longarray-bytebuffer" member-type="[J" datastore-type="java.nio.ByteBuffer"
                    converter-class="org.datanucleus.store.types.converters.LongArrayByteBufferConverter"/>
    <type-converter name="dn.shortarray-bytebuffer" member-type="[S" datastore-type="java.nio.ByteBuffer"
                    converter-class="org.datanucleus.store.types.converters.ShortArrayByteBufferConverter"/>
    <type-converter name="dn.bigintegerarray-bytebuffer" member-type="java.math.BigInteger" datastore-type="java.nio.ByteBuffer"
                    converter-class="org.datanucleus.store.types.converters.BigIntegerArrayByteBufferConverter"/>
    <type-converter name="dn.bigdecimalarray-bytebuffer" member-type="java.math.BigDecimal" datastore-type="java.nio.ByteBuffer"
                    converter-class="org.datanucleus.store.types.converters.BigDecimalArrayByteBufferConverter"/>
  </extension>

  <!-- VALUE GENERATORS -->
  <extension point="org.datanucleus.store_valuegenerator">
    <valuegenerator name="timestamp" class-name="org.datanucleus.store.valuegenerator.TimestampGenerator" unique="true"/>
    <valuegenerator name="timestamp-value" class-name="org.datanucleus.store.valuegenerator.TimestampValueGenerator" unique="true"/>
    <valuegenerator name="auid" class-name="org.datanucleus.store.valuegenerator.AUIDGenerator" unique="true"/>
    <valuegenerator name="uuid-hex" class-name="org.datanucleus.store.valuegenerator.UUIDHexGenerator" unique="true"/>
    <valuegenerator name="uuid-string" class-name="org.datanucleus.store.valuegenerator.UUIDStringGenerator" unique="true"/>
    <valuegenerator name="uuid" class-name="org.datanucleus.store.valuegenerator.UUIDGenerator" unique="true"/>

    <!-- merged -->
    <valuegenerator name="datastore-uuid-hex" class-name="org.datanucleus.store.rdbms.valuegenerator.DatastoreUUIDHexGenerator" datastore="rdbms"/>
    <valuegenerator name="increment" class-name="org.datanucleus.store.rdbms.valuegenerator.TableGenerator" datastore="rdbms"/>
    <valuegenerator name="max" class-name="org.datanucleus.store.rdbms.valuegenerator.MaxGenerator" datastore="rdbms"/>
    <valuegenerator name="sequence" class-name="org.datanucleus.store.rdbms.valuegenerator.SequenceGenerator" datastore="rdbms"/>
    <valuegenerator name="table-sequence" class-name="org.datanucleus.store.rdbms.valuegenerator.TableGenerator" datastore="rdbms"/>
  </extension>

  <!-- CLASS LOADERS -->
  <extension point="org.datanucleus.classloader_resolver">
    <class-loader-resolver name="datanucleus" class-name="org.datanucleus.ClassLoaderResolverImpl"/>
  </extension>

  <!-- AUTOSTART MECHANISMS -->
  <extension point="org.datanucleus.autostart">
    <autostart class-name="org.datanucleus.store.autostart.ClassesAutoStarter" name="Classes"/>
    <autostart class-name="org.datanucleus.store.autostart.XMLAutoStarter" name="XML"/>
    <autostart class-name="org.datanucleus.store.autostart.MetaDataAutoStarter" name="MetaData"/>

    <!-- merged -->
    <autostart class-name="org.datanucleus.store.rdbms.autostart.SchemaAutoStarter" name="SchemaTable"/>
  </extension>

  <!-- QUERY COMPILATION CACHE (GENERIC) -->
  <extension point="org.datanucleus.cache_query_compilation">
    <cache name="soft" class-name="org.datanucleus.query.cache.SoftQueryCompilationCache"/>
    <cache name="weak" class-name="org.datanucleus.query.cache.WeakQueryCompilationCache"/>
    <cache name="strong" class-name="org.datanucleus.query.cache.StrongQueryCompilationCache"/>
  </extension>

  <!-- QUERY COMPILATION CACHE (DATASTORE) -->
  <extension point="org.datanucleus.cache_query_compilation_store">
    <cache name="soft" class-name="org.datanucleus.store.query.cache.SoftQueryDatastoreCompilationCache"/>
    <cache name="weak" class-name="org.datanucleus.store.query.cache.WeakQueryDatastoreCompilationCache"/>
    <cache name="strong" class-name="org.datanucleus.store.query.cache.StrongQueryDatastoreCompilationCache"/>
  </extension>

  <!-- QUERY RESULT CACHE -->
  <extension point="org.datanucleus.cache_query_result">
    <cache name="soft" class-name="org.datanucleus.store.query.cache.SoftQueryResultsCache"/>
    <cache name="weak" class-name="org.datanucleus.store.query.cache.WeakQueryResultsCache"/>
    <cache name="strong" class-name="org.datanucleus.store.query.cache.StrongQueryResultsCache"/>
    <cache name="javax.cache" class-name="org.datanucleus.cache.JavaxCacheQueryResultCache"/>
  </extension>

  <!-- QUERY METHODS (IN-MEMORY) -->
  <extension point="org.datanucleus.query_method_evaluators">
    <query-method-evaluator method="Math.abs" evaluator="org.datanucleus.query.inmemory.AbsFunction"/>
    <query-method-evaluator method="Math.sqrt" evaluator="org.datanucleus.query.inmemory.SqrtFunction"/>
    <query-method-evaluator method="Math.acos" evaluator="org.datanucleus.query.inmemory.ArcCosineFunction"/>
    <query-method-evaluator method="Math.asin" evaluator="org.datanucleus.query.inmemory.ArcSineFunction"/>
    <query-method-evaluator method="Math.atan" evaluator="org.datanucleus.query.inmemory.ArcTangentFunction"/>
    <query-method-evaluator method="Math.cos" evaluator="org.datanucleus.query.inmemory.CosineFunction"/>
    <query-method-evaluator method="Math.sin" evaluator="org.datanucleus.query.inmemory.SineFunction"/>
    <query-method-evaluator method="Math.tan" evaluator="org.datanucleus.query.inmemory.TangentFunction"/>
    <query-method-evaluator method="Math.log" evaluator="org.datanucleus.query.inmemory.LogFunction"/>
    <query-method-evaluator method="Math.exp" evaluator="org.datanucleus.query.inmemory.ExpFunction"/>
    <query-method-evaluator method="Math.floor" evaluator="org.datanucleus.query.inmemory.FloorFunction"/>
    <query-method-evaluator method="Math.ceil" evaluator="org.datanucleus.query.inmemory.CeilFunction"/>
    <query-method-evaluator method="Math.toDegrees" evaluator="org.datanucleus.query.inmemory.DegreesFunction"/>
    <query-method-evaluator method="Math.toRadians" evaluator="org.datanucleus.query.inmemory.RadiansFunction"/>
    <query-method-evaluator method="CURRENT_DATE" evaluator="org.datanucleus.query.inmemory.CurrentDateFunction"/>
    <query-method-evaluator method="CURRENT_TIME" evaluator="org.datanucleus.query.inmemory.CurrentTimeFunction"/>
    <query-method-evaluator method="CURRENT_TIMESTAMP" evaluator="org.datanucleus.query.inmemory.CurrentTimestampFunction"/>
    <query-method-evaluator method="ABS" evaluator="org.datanucleus.query.inmemory.AbsFunction"/>
    <query-method-evaluator method="SQRT" evaluator="org.datanucleus.query.inmemory.SqrtFunction"/>
    <query-method-evaluator method="MOD" evaluator="org.datanucleus.query.inmemory.ModFunction"/>
    <query-method-evaluator method="COALESCE" evaluator="org.datanucleus.query.inmemory.CoalesceFunction"/>
    <query-method-evaluator method="COS" evaluator="org.datanucleus.query.inmemory.CosineFunction"/>
    <query-method-evaluator method="SIN" evaluator="org.datanucleus.query.inmemory.SineFunction"/>
    <query-method-evaluator method="TAN" evaluator="org.datanucleus.query.inmemory.TangentFunction"/>
    <query-method-evaluator method="ACOS" evaluator="org.datanucleus.query.inmemory.ArcCosineFunction"/>
    <query-method-evaluator method="ASIN" evaluator="org.datanucleus.query.inmemory.ArcSineFunction"/>
    <query-method-evaluator method="ATAN" evaluator="org.datanucleus.query.inmemory.ArcTangentFunction"/>
    <query-method-evaluator method="CEIL" evaluator="org.datanucleus.query.inmemory.CeilFunction"/>
    <query-method-evaluator method="FLOOR" evaluator="org.datanucleus.query.inmemory.FloorFunction"/>
    <query-method-evaluator method="LOG" evaluator="org.datanucleus.query.inmemory.LogFunction"/>
    <query-method-evaluator method="EXP" evaluator="org.datanucleus.query.inmemory.ExpFunction"/>
    <query-method-evaluator method="NULLIF" evaluator="org.datanucleus.query.inmemory.NullIfFunction"/>
    <query-method-evaluator method="SIZE" evaluator="org.datanucleus.query.inmemory.SizeFunction"/>
    <query-method-evaluator method="UPPER" evaluator="org.datanucleus.query.inmemory.UpperFunction"/>
    <query-method-evaluator method="LOWER" evaluator="org.datanucleus.query.inmemory.LowerFunction"/>
    <query-method-evaluator method="LENGTH" evaluator="org.datanucleus.query.inmemory.LengthFunction"/>
    <query-method-evaluator method="CONCAT" evaluator="org.datanucleus.query.inmemory.ConcatFunction"/>
    <query-method-evaluator method="SUBSTRING" evaluator="org.datanucleus.query.inmemory.SubstringFunction"/>
    <query-method-evaluator method="LOCATE" evaluator="org.datanucleus.query.inmemory.LocateFunction"/>
    <query-method-evaluator method="TRIM" evaluator="org.datanucleus.query.inmemory.TrimFunction"/>
    <query-method-evaluator method="TRIM_LEADING" evaluator="org.datanucleus.query.inmemory.TrimFunction"/>
    <query-method-evaluator method="TRIM_TRAILING" evaluator="org.datanucleus.query.inmemory.TrimFunction"/>
    <query-method-evaluator method="DEGREES" evaluator="org.datanucleus.query.inmemory.DegreesFunction"/>
    <query-method-evaluator method="RADIANS" evaluator="org.datanucleus.query.inmemory.RadiansFunction"/>

    <query-method-evaluator class="java.lang.Enum" method="matches" evaluator="org.datanucleus.query.inmemory.EnumMatchesMethod"/>
    <query-method-evaluator class="java.lang.Enum" method="toString" evaluator="org.datanucleus.query.inmemory.EnumToStringMethod"/>
    <query-method-evaluator class="java.lang.Enum" method="ordinal" evaluator="org.datanucleus.query.inmemory.EnumOrdinalMethod"/>

    <query-method-evaluator class="java.lang.Object" method="getClass" evaluator="org.datanucleus.query.inmemory.ObjectGetClassMethod"/>

    <query-method-evaluator class="java.lang.String" method="charAt" evaluator="org.datanucleus.query.inmemory.StringCharAtMethod"/>
    <query-method-evaluator class="java.lang.String" method="concat" evaluator="org.datanucleus.query.inmemory.StringConcatMethod"/>
    <query-method-evaluator class="java.lang.String" method="endsWith" evaluator="org.datanucleus.query.inmemory.StringEndsWithMethod"/>
    <query-method-evaluator class="java.lang.String" method="equals" evaluator="org.datanucleus.query.inmemory.StringEqualsMethod"/>
    <query-method-evaluator class="java.lang.String" method="equalsIgnoreCase" evaluator="org.datanucleus.query.inmemory.StringEqualsIgnoreCaseMethod"/>
    <query-method-evaluator class="java.lang.String" method="indexOf" evaluator="org.datanucleus.query.inmemory.StringIndexOfMethod"/>
    <query-method-evaluator class="java.lang.String" method="length" evaluator="org.datanucleus.query.inmemory.StringLengthMethod"/>
    <query-method-evaluator class="java.lang.String" method="matches" evaluator="org.datanucleus.query.inmemory.StringMatchesMethod"/>
    <query-method-evaluator class="java.lang.String" method="startsWith" evaluator="org.datanucleus.query.inmemory.StringStartsWithMethod"/>
    <query-method-evaluator class="java.lang.String" method="substring" evaluator="org.datanucleus.query.inmemory.StringSubstringMethod"/>
    <query-method-evaluator class="java.lang.String" method="toUpperCase" evaluator="org.datanucleus.query.inmemory.StringToUpperCaseMethod"/>
    <query-method-evaluator class="java.lang.String" method="toLowerCase" evaluator="org.datanucleus.query.inmemory.StringToLowerCaseMethod"/>
    <query-method-evaluator class="java.lang.String" method="trim" evaluator="org.datanucleus.query.inmemory.StringTrimMethod"/>
    <query-method-evaluator class="java.lang.String" method="trimLeft" evaluator="org.datanucleus.query.inmemory.StringTrimLeftMethod"/>
    <query-method-evaluator class="java.lang.String" method="trimRight" evaluator="org.datanucleus.query.inmemory.StringTrimRightMethod"/>

    <query-method-evaluator class="java.util.Collection" method="size" evaluator="org.datanucleus.query.inmemory.ContainerSizeMethod"/>
    <query-method-evaluator class="java.util.Collection" method="isEmpty" evaluator="org.datanucleus.query.inmemory.ContainerIsEmptyMethod"/>
    <query-method-evaluator class="java.util.Collection" method="contains" evaluator="org.datanucleus.query.inmemory.CollectionContainsMethod"/>
    <query-method-evaluator class="java.util.Map" method="size" evaluator="org.datanucleus.query.inmemory.ContainerSizeMethod"/>
    <query-method-evaluator class="java.util.Map" method="isEmpty" evaluator="org.datanucleus.query.inmemory.ContainerIsEmptyMethod"/>
    <query-method-evaluator class="java.util.Map" method="containsKey" evaluator="org.datanucleus.query.inmemory.MapContainsKeyMethod"/>
    <query-method-evaluator class="java.util.Map" method="containsValue" evaluator="org.datanucleus.query.inmemory.MapContainsValueMethod"/>
    <query-method-evaluator class="java.util.Map" method="containsEntry" evaluator="org.datanucleus.query.inmemory.MapContainsEntryMethod"/>
    <query-method-evaluator class="java.util.Map" method="get" evaluator="org.datanucleus.query.inmemory.MapGetMethod"/>

    <query-method-evaluator class="java.util.Date" method="getTime" evaluator="org.datanucleus.query.inmemory.DateGetTimeMethod"/>
    <query-method-evaluator class="java.util.Date" method="getDay" evaluator="org.datanucleus.query.inmemory.DateGetDayMethod"/>
    <query-method-evaluator class="java.util.Date" method="getDate" evaluator="org.datanucleus.query.inmemory.DateGetDayMethod"/>
    <query-method-evaluator class="java.util.Date" method="getMonth" evaluator="org.datanucleus.query.inmemory.DateGetMonthMethod"/>
    <query-method-evaluator class="java.util.Date" method="getYear" evaluator="org.datanucleus.query.inmemory.DateGetYearMethod"/>
    <query-method-evaluator class="java.util.Date" method="getHour" evaluator="org.datanucleus.query.inmemory.DateGetHoursMethod"/>
    <query-method-evaluator class="java.util.Date" method="getMinute" evaluator="org.datanucleus.query.inmemory.DateGetMinutesMethod"/>
    <query-method-evaluator class="java.util.Date" method="getSecond" evaluator="org.datanucleus.query.inmemory.DateGetSecondsMethod"/>

    <!-- merged -->
    <query-method-evaluator method="JDOHelper.getObjectId" evaluator="org.datanucleus.api.jdo.query.inmemory.JDOHelperGetObjectIdFunction"/>
    <query-method-evaluator method="JDOHelper.getVersion" evaluator="org.datanucleus.api.jdo.query.inmemory.JDOHelperGetVersionFunction"/>
  </extension>

  <!-- QUERY METHODS -->
  <extension point="org.datanucleus.query_method_prefix">
    <query-method-prefix prefix="JDOHelper" alias="JDOHelper"/>
    <query-method-prefix prefix="javax.jdo.JDOHelper" alias="JDOHelper"/>
    <query-method-prefix prefix="Math" alias="Math"/>
    <query-method-prefix prefix="java.lang.Math" alias="Math"/>
  </extension>

  <!-- METADATA HANDLERS -->
  <extension point="org.datanucleus.metadata_handler">
    <handler class-name="org.datanucleus.metadata.xml.PersistenceFileMetaDataHandler" name="Persistence"
             entity-resolver="org.datanucleus.metadata.xml.PluginEntityResolver"/>

    <!-- merged -->
    <handler class-name="org.datanucleus.api.jdo.metadata.JDOMetaDataHandler" name="JDO"
             entity-resolver="org.datanucleus.metadata.xml.PluginEntityResolver"/>
  </extension>

  <!-- METADATA RESOLVERS -->
  <extension point="org.datanucleus.metadata_entityresolver">
    <entityresolver identity="http://java.sun.com/xml/ns/persistence/persistence_1_0.xsd" type="SYSTEM" url="/org/datanucleus/metadata/persistence_1_0.xsd"/>
    <entityresolver identity="http://xmlns.jcp.org/xml/ns/persistence/persistence_1_0.xsd" type="SYSTEM" url="/org/datanucleus/metadata/persistence_1_0.xsd"/>
    <entityresolver identity="http://java.sun.com/xml/ns/persistence/persistence_2_0.xsd" type="SYSTEM" url="/org/datanucleus/metadata/persistence_2_0.xsd"/>
    <entityresolver identity="http://xmlns.jcp.org/xml/ns/persistence/persistence_2_0.xsd" type="SYSTEM" url="/org/datanucleus/metadata/persistence_2_0.xsd"/>
    <entityresolver identity="http://java.sun.com/xml/ns/persistence/persistence_2_1.xsd" type="SYSTEM" url="/org/datanucleus/metadata/persistence_2_1.xsd"/>
    <entityresolver identity="http://xmlns.jcp.org/xml/ns/persistence/persistence_2_1.xsd" type="SYSTEM" url="/org/datanucleus/metadata/persistence_2_1.xsd"/>
    <entityresolver url="/org/datanucleus/metadata/persistence_2_0.xsd"/>

    <!-- merged -->
    <!-- DTDs -->
    <entityresolver identity="-//Sun Microsystems, Inc.//DTD Java Data Objects Metadata 1.0//EN"
                    type="PUBLIC" url="/org/datanucleus/api/jdo/jdo_1_0.dtd"/>
    <entityresolver identity="-//Sun Microsystems, Inc.//DTD Java Data Objects Metadata 2.0//EN"
                    type="PUBLIC" url="/org/datanucleus/api/jdo/jdo_2_0.dtd"/>
    <entityresolver identity="-//Sun Microsystems, Inc.//DTD Java Data Objects Metadata 2.1//EN"
                    type="PUBLIC" url="/org/datanucleus/api/jdo/jdo_2_0.dtd"/>
    <entityresolver identity="-//Sun Microsystems, Inc.//DTD Java Data Objects Metadata 2.2//EN"
                    type="PUBLIC" url="/org/datanucleus/api/jdo/jdo_2_2.dtd"/>
    <entityresolver identity="-//Sun Microsystems, Inc.//DTD Java Data Objects Metadata 2.3//EN"
                    type="PUBLIC" url="/org/datanucleus/api/jdo/jdo_2_3.dtd"/>
    <entityresolver identity="-//Sun Microsystems, Inc.//DTD Java Data Objects Metadata 3.0//EN"
                    type="PUBLIC" url="/org/datanucleus/api/jdo/jdo_3_0.dtd"/>
    <entityresolver identity="-//Sun Microsystems, Inc.//DTD Java Data Objects Metadata 3.1//EN"
                    type="PUBLIC" url="/org/datanucleus/api/jdo/jdo_3_1.dtd"/>

    <entityresolver identity="-//Sun Microsystems, Inc.//DTD Java Data Objects Mapping Metadata 2.0//EN"
                    type="PUBLIC" url="/org/datanucleus/api/jdo/jdo_orm_2_0.dtd"/>
    <entityresolver identity="-//Sun Microsystems, Inc.//DTD Java Data Objects Mapping Metadata 2.1//EN"
                    type="PUBLIC" url="/org/datanucleus/api/jdo/jdo_orm_2_0.dtd"/>
    <entityresolver identity="-//Sun Microsystems, Inc.//DTD Java Data Objects Mapping Metadata 2.2//EN"
                    type="PUBLIC" url="/org/datanucleus/api/jdo/jdo_orm_2_2.dtd"/>
    <entityresolver identity="-//Sun Microsystems, Inc.//DTD Java Data Objects Mapping Metadata 3.0//EN"
                    type="PUBLIC" url="/org/datanucleus/api/jdo/jdo_orm_3_0.dtd"/>
    <entityresolver identity="-//Sun Microsystems, Inc.//DTD Java Data Objects Mapping Metadata 3.1//EN"
                    type="PUBLIC" url="/org/datanucleus/api/jdo/jdo_orm_3_1.dtd"/>

    <entityresolver identity="-//Sun Microsystems, Inc.//DTD Java Data Objects Query Metadata 2.0//EN"
                    type="PUBLIC" url="/org/datanucleus/api/jdo/jdoquery_2_0.dtd"/>
    <entityresolver identity="-//Sun Microsystems, Inc.//DTD Java Data Objects Query Metadata 2.1//EN"
                    type="PUBLIC" url="/org/datanucleus/api/jdo/jdoquery_2_0.dtd"/>
    <entityresolver identity="-//Sun Microsystems, Inc.//DTD Java Data Objects Query Metadata 2.2//EN"
                    type="PUBLIC" url="/org/datanucleus/api/jdo/jdoquery_2_2.dtd"/>
    <entityresolver identity="-//Sun Microsystems, Inc.//DTD Java Data Objects Query Metadata 3.0//EN"
                    type="PUBLIC" url="/org/datanucleus/api/jdo/jdoquery_3_0.dtd"/>

    <!-- DTD Shortcuts : point to latest version -->
    <entityresolver identity="file:/javax/jdo/jdo.dtd" type="SYSTEM" url="/org/datanucleus/api/jdo/jdo_3_1.dtd"/>
    <entityresolver identity="file:/javax/jdo/orm.dtd" type="SYSTEM" url="/org/datanucleus/api/jdo/jdo_orm_3_1.dtd"/>
    <entityresolver identity="file:/javax/jdo/jdoquery.dtd" type="SYSTEM" url="/org/datanucleus/api/jdo/jdoquery_3_0.dtd"/>

    <!-- XSDs -->
    <entityresolver identity="http://java.sun.com/xml/ns/jdo/jdoquery_3_2.xsd" type="SYSTEM" url="/org/datanucleus/api/jdo/jdoquery_3_0.xsd"/>
    <entityresolver identity="http://java.sun.com/xml/ns/jdo/jdoquery_3_1.xsd" type="SYSTEM" url="/org/datanucleus/api/jdo/jdoquery_3_0.xsd"/>
    <entityresolver identity="http://java.sun.com/xml/ns/jdo/jdoquery_3_0.xsd" type="SYSTEM" url="/org/datanucleus/api/jdo/jdoquery_3_0.xsd"/>
    <entityresolver identity="http://java.sun.com/xml/ns/jdo/jdoquery_2_2.xsd" type="SYSTEM" url="/org/datanucleus/api/jdo/jdoquery_2_2.xsd"/>
    <entityresolver identity="http://java.sun.com/xml/ns/jdo/jdoquery_2_1.xsd" type="SYSTEM" url="/org/datanucleus/api/jdo/jdoquery_2_1.xsd"/>
    <entityresolver identity="http://java.sun.com/xml/ns/jdo/jdoquery_2_0.xsd" type="SYSTEM" url="/org/datanucleus/api/jdo/jdoquery_2_0.xsd"/>
    <entityresolver identity="http://xmlns.jcp.org/xml/ns/jdo/jdoquery_3_2.xsd" type="SYSTEM" url="/org/datanucleus/api/jdo/jdoquery_3_0.xsd"/>
    <entityresolver identity="http://xmlns.jcp.org/xml/ns/jdo/jdoquery_3_1.xsd" type="SYSTEM" url="/org/datanucleus/api/jdo/jdoquery_3_0.xsd"/>
    <entityresolver identity="http://xmlns.jcp.org/xml/ns/jdo/jdoquery_3_0.xsd" type="SYSTEM" url="/org/datanucleus/api/jdo/jdoquery_3_0.xsd"/>
    <entityresolver identity="http://xmlns.jcp.org/xml/ns/jdo/jdoquery_2_2.xsd" type="SYSTEM" url="/org/datanucleus/api/jdo/jdoquery_2_2.xsd"/>
    <entityresolver identity="http://xmlns.jcp.org/xml/ns/jdo/jdoquery_2_1.xsd" type="SYSTEM" url="/org/datanucleus/api/jdo/jdoquery_2_1.xsd"/>
    <entityresolver identity="http://xmlns.jcp.org/xml/ns/jdo/jdoquery_2_0.xsd" type="SYSTEM" url="/org/datanucleus/api/jdo/jdoquery_2_0.xsd"/>

    <entityresolver identity="http://java.sun.com/xml/ns/jdo/orm_3_2.xsd" type="SYSTEM" url="/org/datanucleus/api/jdo/jdo_orm_3_2.xsd"/>
    <entityresolver identity="http://java.sun.com/xml/ns/jdo/orm_3_1.xsd" type="SYSTEM" url="/org/datanucleus/api/jdo/jdo_orm_3_1.xsd"/>
    <entityresolver identity="http://java.sun.com/xml/ns/jdo/orm_3_0.xsd" type="SYSTEM" url="/org/datanucleus/api/jdo/jdo_orm_3_0.xsd"/>
    <entityresolver identity="http://java.sun.com/xml/ns/jdo/orm_2_2.xsd" type="SYSTEM" url="/org/datanucleus/api/jdo/jdo_orm_2_2.xsd"/>
    <entityresolver identity="http://java.sun.com/xml/ns/jdo/orm_2_1.xsd" type="SYSTEM" url="/org/datanucleus/api/jdo/jdo_orm_2_1.xsd"/>
    <entityresolver identity="http://java.sun.com/xml/ns/jdo/orm_2_0.xsd" type="SYSTEM" url="/org/datanucleus/api/jdo/jdo_orm_2_0.xsd"/>
    <entityresolver identity="http://xmlns.jcp.org/xml/ns/jdo/orm_3_2.xsd" type="SYSTEM" url="/org/datanucleus/api/jdo/jdo_orm_3_2.xsd"/>
    <entityresolver identity="http://xmlns.jcp.org/xml/ns/jdo/orm_3_1.xsd" type="SYSTEM" url="/org/datanucleus/api/jdo/jdo_orm_3_1.xsd"/>
    <entityresolver identity="http://xmlns.jcp.org/xml/ns/jdo/orm_3_0.xsd" type="SYSTEM" url="/org/datanucleus/api/jdo/jdo_orm_3_0.xsd"/>
    <entityresolver identity="http://xmlns.jcp.org/xml/ns/jdo/orm_2_2.xsd" type="SYSTEM" url="/org/datanucleus/api/jdo/jdo_orm_2_2.xsd"/>
    <entityresolver identity="http://xmlns.jcp.org/xml/ns/jdo/orm_2_1.xsd" type="SYSTEM" url="/org/datanucleus/api/jdo/jdo_orm_2_1.xsd"/>
    <entityresolver identity="http://xmlns.jcp.org/xml/ns/jdo/orm_2_0.xsd" type="SYSTEM" url="/org/datanucleus/api/jdo/jdo_orm_2_0.xsd"/>

    <entityresolver identity="http://java.sun.com/xml/ns/jdo/jdo_3_2.xsd" type="SYSTEM" url="/org/datanucleus/api/jdo/jdo_3_2.xsd"/>
    <entityresolver identity="http://java.sun.com/xml/ns/jdo/jdo_3_1.xsd" type="SYSTEM" url="/org/datanucleus/api/jdo/jdo_3_1.xsd"/>
    <entityresolver identity="http://java.sun.com/xml/ns/jdo/jdo_3_0.xsd" type="SYSTEM" url="/org/datanucleus/api/jdo/jdo_3_0.xsd"/>
    <entityresolver identity="http://java.sun.com/xml/ns/jdo/jdo_2_2.xsd" type="SYSTEM" url="/org/datanucleus/api/jdo/jdo_2_2.xsd"/>
    <entityresolver identity="http://java.sun.com/xml/ns/jdo/jdo_2_1.xsd" type="SYSTEM" url="/org/datanucleus/api/jdo/jdo_2_1.xsd"/>
    <entityresolver identity="http://java.sun.com/xml/ns/jdo/jdo_2_0.xsd" type="SYSTEM" url="/org/datanucleus/api/jdo/jdo_2_0.xsd"/>
    <entityresolver identity="http://xmlns.jcp.org/xml/ns/jdo/jdo_3_2.xsd" type="SYSTEM" url="/org/datanucleus/api/jdo/jdo_3_2.xsd"/>
    <entityresolver identity="http://xmlns.jcp.org/xml/ns/jdo/jdo_3_1.xsd" type="SYSTEM" url="/org/datanucleus/api/jdo/jdo_3_1.xsd"/>
    <entityresolver identity="http://xmlns.jcp.org/xml/ns/jdo/jdo_3_0.xsd" type="SYSTEM" url="/org/datanucleus/api/jdo/jdo_3_0.xsd"/>
    <entityresolver identity="http://xmlns.jcp.org/xml/ns/jdo/jdo_2_2.xsd" type="SYSTEM" url="/org/datanucleus/api/jdo/jdo_2_2.xsd"/>
    <entityresolver identity="http://xmlns.jcp.org/xml/ns/jdo/jdo_2_1.xsd" type="SYSTEM" url="/org/datanucleus/api/jdo/jdo_2_1.xsd"/>
    <entityresolver identity="http://xmlns.jcp.org/xml/ns/jdo/jdo_2_0.xsd" type="SYSTEM" url="/org/datanucleus/api/jdo/jdo_2_0.xsd"/>

    <entityresolver url="/org/datanucleus/api/jdo/jdoquery_3_2.xsd"/>
    <entityresolver url="/org/datanucleus/api/jdo/jdo_orm_3_2.xsd"/>
    <entityresolver url="/org/datanucleus/api/jdo/jdo_3_2.xsd"/>
  </extension>

  <!-- EXTENSION ANNOTATIONS -->
  <extension point="org.datanucleus.member_annotation_handler">
    <member-annotation-handler annotation-class="javax.validation.constraints.NotNull"
                               handler="org.datanucleus.metadata.annotations.ValidationNotNullAnnotationHandler"/>
    <member-annotation-handler annotation-class="javax.validation.constraints.Size"
                               handler="org.datanucleus.metadata.annotations.ValidationSizeAnnotationHandler"/>
  </extension>

  <!-- JMX SERVERS -->
  <extension point="org.datanucleus.management_server">
    <management-server class="org.datanucleus.management.jmx.PlatformManagementServer" name="default"/>
    <management-server class="org.datanucleus.management.jmx.Mx4jManagementServer" name="mx4j"/>
  </extension>

  <!-- extension points from datanucleus-rdbms-4.1.7 -->
  <!-- STORE MANAGER -->
  <extension point="org.datanucleus.store_manager">
    <store-manager class-name="org.datanucleus.store.rdbms.RDBMSStoreManager" url-key="jdbc" key="rdbms"/>
  </extension>

  <!-- QUERY LANGUAGES -->
  <extension point="org.datanucleus.store_query_query">
    <query class-name="org.datanucleus.store.rdbms.query.JDOQLQuery" datastore="rdbms" name="JDOQL"/>
    <query class-name="org.datanucleus.store.rdbms.query.JPQLQuery" datastore="rdbms" name="JPQL"/>
    <query class-name="org.datanucleus.store.rdbms.query.SQLQuery" datastore="rdbms" name="SQL"/>
    <query class-name="org.datanucleus.store.rdbms.query.StoredProcedureQuery" datastore="rdbms" name="STOREDPROC"/>
  </extension>

  <!-- CONNECTION FACTORY -->
  <extension point="org.datanucleus.store_connectionfactory">
    <connectionfactory name="rdbms/tx" class-name="org.datanucleus.store.rdbms.ConnectionFactoryImpl" transactional="true" datastore="rdbms"/>
    <connectionfactory name="rdbms/nontx" class-name="org.datanucleus.store.rdbms.ConnectionFactoryImpl" transactional="false" datastore="rdbms"/>
  </extension>

  <!-- CONNECTION PROVIDER -->
  <extension point="org.datanucleus.store.rdbms.connectionprovider">
    <connection-provider class-name="org.datanucleus.store.rdbms.ConnectionProviderPriorityList" name="PriorityList"/>
  </extension>

  <!-- CONNECTIONPOOL -->
  <extension point="org.datanucleus.store.rdbms.connectionpool">
    <connectionpool-factory name="dbcp-builtin" class-name="org.datanucleus.store.rdbms.connectionpool.DBCPBuiltinConnectionPoolFactory"/>
    <connectionpool-factory name="None" class-name="org.datanucleus.store.rdbms.connectionpool.DefaultConnectionPoolFactory"/>
    <connectionpool-factory name="BoneCP" class-name="org.datanucleus.store.rdbms.connectionpool.BoneCPConnectionPoolFactory"/>
    <connectionpool-factory name="C3P0" class-name="org.datanucleus.store.rdbms.connectionpool.C3P0ConnectionPoolFactory"/>
    <connectionpool-factory name="DBCP" class-name="org.datanucleus.store.rdbms.connectionpool.DBCPConnectionPoolFactory"/>
    <connectionpool-factory name="DBCP2" class-name="org.datanucleus.store.rdbms.connectionpool.DBCP2ConnectionPoolFactory"/>
    <connectionpool-factory name="Proxool" class-name="org.datanucleus.store.rdbms.connectionpool.ProxoolConnectionPoolFactory"/>
    <connectionpool-factory name="Tomcat" class-name="org.datanucleus.store.rdbms.connectionpool.TomcatConnectionPoolFactory"/>
    <connectionpool-factory name="HikariCP" class-name="org.datanucleus.store.rdbms.connectionpool.HikariCPConnectionPoolFactory"/>
  </extension>

  <!-- PERSISTENCE PROPERTIES -->
  <extension point="org.datanucleus.persistence_properties">
    <persistence-property name="datanucleus.rdbms.useLegacyNativeValueStrategy" datastore="true" value="false"
                          validator="org.datanucleus.properties.BooleanPropertyValidator"/>
    <persistence-property name="datanucleus.rdbms.dynamicSchemaUpdates" datastore="true" value="false"
                          validator="org.datanucleus.properties.BooleanPropertyValidator"/>
    <persistence-property name="datanucleus.rdbms.tableColumnOrder" datastore="true" value="owner-first"/>

    <persistence-property name="datanucleus.rdbms.query.fetchDirection" datastore="true" value="forward"
                          validator="org.datanucleus.store.rdbms.RDBMSPropertyValidator"/>
    <persistence-property name="datanucleus.rdbms.query.resultSetType" datastore="true" value="forward-only"
                          validator="org.datanucleus.store.rdbms.RDBMSPropertyValidator"/>
    <persistence-property name="datanucleus.rdbms.query.resultSetConcurrency" datastore="true" value="read-only"
                          validator="org.datanucleus.store.rdbms.RDBMSPropertyValidator"/>
    <persistence-property name="datanucleus.rdbms.query.multivaluedFetch" datastore="true" value="exists"
                          validator="org.datanucleus.store.rdbms.RDBMSPropertyValidator"/>

    <persistence-property name="datanucleus.rdbms.classAdditionMaxRetries" datastore="true" value="3"
                          validator="org.datanucleus.properties.IntegerPropertyValidator"/>
    <persistence-property name="datanucleus.rdbms.statementBatchLimit" datastore="true" value="50"
                          validator="org.datanucleus.properties.IntegerPropertyValidator"/>
    <persistence-property name="datanucleus.rdbms.oracleNlsSortOrder" datastore="true" value="LATIN"/>
    <persistence-property name="datanucleus.rdbms.discriminatorPerSubclassTable" datastore="true" value="false"
                          validator="org.datanucleus.properties.BooleanPropertyValidator"/>
    <persistence-property name="datanucleus.rdbms.constraintCreateMode" datastore="true" value="DataNucleus"
                          validator="org.datanucleus.store.rdbms.RDBMSPropertyValidator"/>
    <persistence-property name="datanucleus.rdbms.uniqueConstraints.mapInverse" datastore="true" value="true"
                          validator="org.datanucleus.properties.BooleanPropertyValidator"/>
    <persistence-property name="datanucleus.rdbms.initializeColumnInfo" datastore="true" value="ALL"
                          validator="org.datanucleus.store.rdbms.RDBMSPropertyValidator"/>
    <persistence-property name="datanucleus.rdbms.useDefaultSqlType" datastore="true" value="true"
                          validator="org.datanucleus.properties.BooleanPropertyValidator"/>
    <persistence-property name="datanucleus.rdbms.stringDefaultLength" datastore="true" value="255"
                          validator="org.datanucleus.properties.IntegerPropertyValidator"/>
    <persistence-property name="datanucleus.rdbms.stringLengthExceededAction" datastore="true" value="EXCEPTION"
                          validator="org.datanucleus.store.rdbms.RDBMSPropertyValidator"/>
    <persistence-property name="datanucleus.rdbms.persistEmptyStringAsNull" datastore="true" value="false"
                          validator="org.datanucleus.properties.BooleanPropertyValidator"/>
    <persistence-property name="datanucleus.rdbms.checkExistTablesOrViews" datastore="true" value="true"
                          validator="org.datanucleus.properties.BooleanPropertyValidator"/>
    <persistence-property name="datanucleus.rdbms.schemaTable.tableName" datastore="true"/>
    <persistence-property name="datanucleus.rdbms.connectionProviderName" datastore="true" value="PriorityList"/>
    <persistence-property name="datanucleus.rdbms.connectionProviderFailOnError" datastore="true" value="false"
                          validator="org.datanucleus.properties.BooleanPropertyValidator"/>
    <persistence-property name="datanucleus.rdbms.datastoreAdapterClassName" datastore="true"/>
    <persistence-property name="datanucleus.rdbms.omitDatabaseMetaDataGetColumns" datastore="true" value="false"
                          validator="org.datanucleus.properties.BooleanPropertyValidator"/>
    <persistence-property name="datanucleus.rdbms.sqlTableNamingStrategy" datastore="true" value="alpha-scheme"/>
    <persistence-property name="datanucleus.rdbms.allowColumnReuse" datastore="true" value="false"/>

    <persistence-property name="datanucleus.rdbms.adapter.informixUseSerialForIdentity" datastore="true"
                          validator="org.datanucleus.properties.BooleanPropertyValidator"/>
    <persistence-property name="datanucleus.rdbms.statementLogging" datastore="true" value="values-in-brackets"
                          validator="org.datanucleus.store.rdbms.RDBMSPropertyValidator"/>
    <persistence-property name="datanucleus.rdbms.fetchUnloadedAutomatically" datastore="true" value="false"
                          validator="org.datanucleus.properties.BooleanPropertyValidator"/>

    <persistence-property name="datanucleus.rdbms.mysql.engineType" datastore="true"/>
    <persistence-property name="datanucleus.rdbms.mysql.collation" datastore="true"/>
    <persistence-property name="datanucleus.rdbms.mysql.characterSet" datastore="true"/>

    <!-- TODO Rename these to "datanucleus.rdbms.connectionPool.*" or move to "core" -->
    <persistence-property name="datanucleus.connectionPool.maxStatements" datastore="true" value="0"
                          validator="org.datanucleus.properties.IntegerPropertyValidator"/>
    <persistence-property name="datanucleus.connectionPool.maxPoolSize" datastore="true"
                          validator="org.datanucleus.properties.IntegerPropertyValidator"/>
    <persistence-property name="datanucleus.connectionPool.minPoolSize" datastore="true"
                          validator="org.datanucleus.properties.IntegerPropertyValidator"/>
    <persistence-property name="datanucleus.connectionPool.initialPoolSize" datastore="true"
                          validator="org.datanucleus.properties.IntegerPropertyValidator"/>
    <persistence-property name="datanucleus.connectionPool.maxIdle" datastore="true"
                          validator="org.datanucleus.properties.IntegerPropertyValidator"/>
    <persistence-property name="datanucleus.connectionPool.minIdle" datastore="true"
                          validator="org.datanucleus.properties.IntegerPropertyValidator"/>
    <persistence-property name="datanucleus.connectionPool.maxActive" datastore="true"
                          validator="org.datanucleus.properties.IntegerPropertyValidator"/>
    <persistence-property name="datanucleus.connectionPool.maxWait" datastore="true"
                          validator="org.datanucleus.properties.IntegerPropertyValidator"/>
    <persistence-property name="datanucleus.connectionPool.testSQL" datastore="true"
                          validator="org.datanucleus.properties.StringPropertyValidator"/>
    <persistence-property name="datanucleus.connectionPool.timeBetweenEvictionRunsMillis" datastore="true"
                          validator="org.datanucleus.properties.IntegerPropertyValidator"/>
    <persistence-property name="datanucleus.connectionPool.minEvictableIdleTimeMillis" datastore="true"
                          validator="org.datanucleus.properties.IntegerPropertyValidator"/>
    <persistence-property name="datanucleus.connectionPool.maxConnections" datastore="true"
                          validator="org.datanucleus.properties.IntegerPropertyValidator"/>
    <persistence-property name="datanucleus.connectionPool.driverProps"/>

    <!-- merged -->
    <persistence-property name="javax.jdo.PersistenceManagerFactoryClass"/>
    <persistence-property name="javax.jdo.option.ConnectionDriverName" internal-name="datanucleus.ConnectionDriverName"/>
    <persistence-property name="javax.jdo.option.ConnectionURL" internal-name="datanucleus.ConnectionURL"/>
    <persistence-property name="javax.jdo.option.ConnectionUserName" internal-name="datanucleus.ConnectionUserName"/>
    <persistence-property name="javax.jdo.option.ConnectionPassword" internal-name="datanucleus.ConnectionPassword"/>
    <persistence-property name="javax.jdo.option.ConnectionFactoryName" internal-name="datanucleus.ConnectionFactoryName"/>
    <persistence-property name="javax.jdo.option.ConnectionFactory2Name" internal-name="datanucleus.ConnectionFactory2Name"/>
    <persistence-property name="javax.jdo.option.ConnectionFactory" internal-name="datanucleus.ConnectionFactory"/>
    <persistence-property name="javax.jdo.option.ConnectionFactory2" internal-name="datanucleus.ConnectionFactory2"/>
    <persistence-property name="javax.jdo.option.IgnoreCache" internal-name="datanucleus.IgnoreCache"
                          manager-overrideable="true" validator="org.datanucleus.properties.BooleanPropertyValidator"/>
    <persistence-property name="javax.jdo.option.Optimistic" internal-name="datanucleus.Optimistic"
                          manager-overrideable="true" validator="org.datanucleus.properties.BooleanPropertyValidator"/>
    <persistence-property name="javax.jdo.option.Multithreaded" internal-name="datanucleus.Multithreaded"
                          manager-overrideable="true" validator="org.datanucleus.properties.BooleanPropertyValidator"/>
    <persistence-property name="javax.jdo.option.RetainValues" internal-name="datanucleus.RetainValues"
                          manager-overrideable="true" validator="org.datanucleus.properties.BooleanPropertyValidator"/>
    <persistence-property name="javax.jdo.option.RestoreValues" internal-name="datanucleus.RestoreValues"
                          manager-overrideable="true" validator="org.datanucleus.properties.BooleanPropertyValidator"/>
    <persistence-property name="javax.jdo.option.NontransactionalRead" internal-name="datanucleus.NontransactionalRead"
                          manager-overrideable="true" validator="org.datanucleus.properties.BooleanPropertyValidator"/>
    <persistence-property name="javax.jdo.option.NontransactionalWrite" internal-name="datanucleus.NontransactionalWrite"
                          manager-overrideable="true" validator="org.datanucleus.properties.BooleanPropertyValidator"/>
    <persistence-property name="javax.jdo.option.DetachAllOnCommit" internal-name="datanucleus.DetachAllOnCommit"
                          manager-overrideable="true" validator="org.datanucleus.properties.BooleanPropertyValidator"/>
    <persistence-property name="javax.jdo.option.CopyOnAttach" internal-name="datanucleus.CopyOnAttach"
                          manager-overrideable="true" validator="org.datanucleus.properties.BooleanPropertyValidator"/>
    <persistence-property name="javax.jdo.option.TransactionType" internal-name="datanucleus.TransactionType"
                          validator="org.datanucleus.properties.CorePropertyValidator"/>
    <persistence-property name="javax.jdo.option.Name" internal-name="datanucleus.Name"
                          validator="org.datanucleus.properties.StringPropertyValidator"/>
    <persistence-property name="javax.jdo.option.PersistenceUnitName" internal-name="datanucleus.PersistenceUnitName"
                          validator="org.datanucleus.properties.StringPropertyValidator"/>
    <persistence-property name="javax.jdo.option.ServerTimeZoneID" internal-name="datanucleus.ServerTimeZoneID"
                          validator="org.datanucleus.properties.CorePropertyValidator"/>
    <persistence-property name="javax.jdo.option.ReadOnly" internal-name="datanucleus.readOnlyDatastore"
                          validator="org.datanucleus.properties.BooleanPropertyValidator"/>
    <persistence-property name="javax.jdo.option.TransactionIsolationLevel" internal-name="datanucleus.transactionIsolation"
                          validator="org.datanucleus.properties.CorePropertyValidator"/>
    <persistence-property name="javax.jdo.option.Mapping" internal-name="datanucleus.Mapping"
                          validator="org.datanucleus.properties.StringPropertyValidator"/>
    <persistence-property name="javax.jdo.mapping.Catalog" internal-name="datanucleus.mapping.Catalog"
                          validator="org.datanucleus.properties.StringPropertyValidator"/>
    <persistence-property name="javax.jdo.mapping.Schema" internal-name="datanucleus.mapping.Schema"
                          validator="org.datanucleus.properties.StringPropertyValidator"/>
    <persistence-property name="javax.jdo.option.DatastoreReadTimeoutMillis" internal-name="datanucleus.datastoreReadTimeout" value="0"
                          manager-overrideable="true" validator="org.datanucleus.properties.IntegerPropertyValidator"/>
    <persistence-property name="javax.jdo.option.DatastoreWriteTimeoutMillis" internal-name="datanucleus.datastoreWriteTimeout" value="0"
                          manager-overrideable="true" validator="org.datanucleus.properties.IntegerPropertyValidator"/>
    <persistence-property name="javax.jdo.option.spi.ResourceName"/> <!-- JDOHelper nonsense -->
    <persistence-property name="javax.jdo.option.Multitenancy" internal-name="datanucleus.tenantID"/>

    <persistence-property name="datanucleus.allowListenerUpdateAfterInit" value="false"
                          validator="org.datanucleus.properties.BooleanPropertyValidator"/>

    <persistence-property name="datanucleus.singletonPMFForName" value="false"
                          validator="org.datanucleus.properties.BooleanPropertyValidator"/>
  </extension>

  <!-- RDBMS : JAVA TYPES MAPPING -->
  <extension point="org.datanucleus.store.rdbms.java_mapping">
    <!-- "primitive" types -->
    <mapping java-type="boolean" mapping-class="org.datanucleus.store.rdbms.mapping.java.BooleanMapping"/>
    <mapping java-type="byte" mapping-class="org.datanucleus.store.rdbms.mapping.java.ByteMapping"/>
    <mapping java-type="char" mapping-class="org.datanucleus.store.rdbms.mapping.java.CharacterMapping"/>
    <mapping java-type="double" mapping-class="org.datanucleus.store.rdbms.mapping.java.DoubleMapping"/>
    <mapping java-type="float" mapping-class="org.datanucleus.store.rdbms.mapping.java.FloatMapping" />
    <mapping java-type="int" mapping-class="org.datanucleus.store.rdbms.mapping.java.IntegerMapping"/>
    <mapping java-type="long" mapping-class="org.datanucleus.store.rdbms.mapping.java.LongMapping"/>
    <mapping java-type="short" mapping-class="org.datanucleus.store.rdbms.mapping.java.ShortMapping"/>

    <!-- "java.lang" types -->
    <mapping java-type="java.lang.Boolean" mapping-class="org.datanucleus.store.rdbms.mapping.java.BooleanMapping"/>
    <mapping java-type="java.lang.Byte" mapping-class="org.datanucleus.store.rdbms.mapping.java.ByteMapping"/>
    <mapping java-type="java.lang.Character" mapping-class="org.datanucleus.store.rdbms.mapping.java.CharacterMapping"/>
    <mapping java-type="java.lang.Double" mapping-class="org.datanucleus.store.rdbms.mapping.java.DoubleMapping"/>
    <mapping java-type="java.lang.Float" mapping-class="org.datanucleus.store.rdbms.mapping.java.FloatMapping"/>
    <mapping java-type="java.lang.Integer" mapping-class="org.datanucleus.store.rdbms.mapping.java.IntegerMapping"/>
    <mapping java-type="java.lang.Long" mapping-class="org.datanucleus.store.rdbms.mapping.java.LongMapping"/>
    <mapping java-type="java.lang.Short" mapping-class="org.datanucleus.store.rdbms.mapping.java.ShortMapping"/>

    <mapping java-type="java.lang.Class" mapping-class="org.datanucleus.store.rdbms.mapping.java.ClassMapping"/>
    <mapping java-type="java.lang.Number" mapping-class="org.datanucleus.store.rdbms.mapping.java.NumberMapping"/>
    <mapping java-type="java.lang.Object" mapping-class="org.datanucleus.store.rdbms.mapping.java.SerialisedMapping"/>
    <mapping java-type="java.lang.String" mapping-class="org.datanucleus.store.rdbms.mapping.java.StringMapping"/>
    <mapping java-type="java.lang.Enum" mapping-class="org.datanucleus.store.rdbms.mapping.java.EnumMapping"/>

    <!-- "java.awt" types -->
    <mapping java-type="java.awt.image.BufferedImage" mapping-class="org.datanucleus.store.rdbms.mapping.java.BufferedImageMapping"/>

    <!-- "java.io" types -->
    <mapping java-type="java.io.File" mapping-class="org.datanucleus.store.rdbms.mapping.java.FileMapping"/>
    <mapping java-type="java.io.Serializable" mapping-class="org.datanucleus.store.rdbms.mapping.java.SerialisedMapping"/>

    <!-- "java.math" types -->
    <mapping java-type="java.math.BigDecimal" mapping-class="org.datanucleus.store.rdbms.mapping.java.BigDecimalMapping"/>
    <mapping java-type="java.math.BigInteger" mapping-class="org.datanucleus.store.rdbms.mapping.java.BigIntegerMapping"/>

    <!-- "java.util" types -->
    <mapping java-type="java.util.Calendar" mapping-class="org.datanucleus.store.rdbms.mapping.java.GregorianCalendarMapping"/>
    <mapping java-type="java.util.Date" mapping-class="org.datanucleus.store.rdbms.mapping.java.DateMapping"/>
    <mapping java-type="java.util.UUID" mapping-class="org.datanucleus.store.rdbms.mapping.java.UUIDMapping"/>

    <!-- "java.sql" types -->
    <mapping java-type="java.sql.Date" mapping-class="org.datanucleus.store.rdbms.mapping.java.SqlDateMapping"/>
    <mapping java-type="java.sql.Time" mapping-class="org.datanucleus.store.rdbms.mapping.java.SqlTimeMapping"/>
    <mapping java-type="java.sql.Timestamp" mapping-class="org.datanucleus.store.rdbms.mapping.java.SqlTimestampMapping"/>

    <!-- array types -->
    <mapping java-type="[B" mapping-class="org.datanucleus.store.rdbms.mapping.java.ArrayMapping"/>
    <mapping java-type="[C" mapping-class="org.datanucleus.store.rdbms.mapping.java.ArrayMapping"/>
    <mapping java-type="[D" mapping-class="org.datanucleus.store.rdbms.mapping.java.ArrayMapping"/>
    <mapping java-type="[F" mapping-class="org.datanucleus.store.rdbms.mapping.java.ArrayMapping"/>
    <mapping java-type="[I" mapping-class="org.datanucleus.store.rdbms.mapping.java.ArrayMapping"/>
    <mapping java-type="[J" mapping-class="org.datanucleus.store.rdbms.mapping.java.ArrayMapping"/>
    <mapping java-type="[S" mapping-class="org.datanucleus.store.rdbms.mapping.java.ArrayMapping"/>
    <mapping java-type="[Z" mapping-class="org.datanucleus.store.rdbms.mapping.java.ArrayMapping"/>

    <mapping java-type="[Ljava.lang.Boolean;" mapping-class="org.datanucleus.store.rdbms.mapping.java.ArrayMapping"/>
    <mapping java-type="[Ljava.lang.Byte;" mapping-class="org.datanucleus.store.rdbms.mapping.java.ArrayMapping"/>
    <mapping java-type="[Ljava.lang.Character;" mapping-class="org.datanucleus.store.rdbms.mapping.java.ArrayMapping"/>
    <mapping java-type="[Ljava.lang.Double;" mapping-class="org.datanucleus.store.rdbms.mapping.java.ArrayMapping"/>
    <mapping java-type="[Ljava.lang.Float;" mapping-class="org.datanucleus.store.rdbms.mapping.java.ArrayMapping"/>
    <mapping java-type="[Ljava.lang.Integer;" mapping-class="org.datanucleus.store.rdbms.mapping.java.ArrayMapping"/>
    <mapping java-type="[Ljava.lang.Long;" mapping-class="org.datanucleus.store.rdbms.mapping.java.ArrayMapping"/>
    <mapping java-type="[Ljava.lang.Short;" mapping-class="org.datanucleus.store.rdbms.mapping.java.ArrayMapping"/>

    <mapping java-type="[Ljava.lang.Number;" mapping-class="org.datanucleus.store.rdbms.mapping.java.ArrayMapping"/>
    <mapping java-type="[Ljava.lang.String;" mapping-class="org.datanucleus.store.rdbms.mapping.java.ArrayMapping"/>
    <mapping java-type="[Ljava.lang.Enum;" mapping-class="org.datanucleus.store.rdbms.mapping.java.ArrayMapping"/>
    <mapping java-type="[Ljava.math.BigInteger;" mapping-class="org.datanucleus.store.rdbms.mapping.java.ArrayMapping"/>
    <mapping java-type="[Ljava.math.BigDecimal;" mapping-class="org.datanucleus.store.rdbms.mapping.java.ArrayMapping"/>
    <mapping java-type="[Ljava.util.Date;" mapping-class="org.datanucleus.store.rdbms.mapping.java.ArrayMapping"/>
    <mapping java-type="[Ljava.util.Locale;" mapping-class="org.datanucleus.store.rdbms.mapping.java.ArrayMapping"/>

    <!-- container types -->
    <mapping java-type="java.util.ArrayList" mapping-class="org.datanucleus.store.rdbms.mapping.java.CollectionMapping"/>
    <mapping java-type="java.util.BitSet" mapping-class="org.datanucleus.store.rdbms.mapping.java.BitSetMapping"/>
    <mapping java-type="java.util.Collection" mapping-class="org.datanucleus.store.rdbms.mapping.java.CollectionMapping"/>
    <mapping java-type="java.util.GregorianCalendar" mapping-class="org.datanucleus.store.rdbms.mapping.java.GregorianCalendarMapping"/>
    <mapping java-type="java.util.HashMap" mapping-class="org.datanucleus.store.rdbms.mapping.java.MapMapping"/>
    <mapping java-type="java.util.HashSet" mapping-class="org.datanucleus.store.rdbms.mapping.java.CollectionMapping"/>
    <mapping java-type="java.util.Hashtable" mapping-class="org.datanucleus.store.rdbms.mapping.java.MapMapping"/>
    <mapping java-type="java.util.LinkedList" mapping-class="org.datanucleus.store.rdbms.mapping.java.CollectionMapping"/>
    <mapping java-type="java.util.List" mapping-class="org.datanucleus.store.rdbms.mapping.java.CollectionMapping"/>
    <mapping java-type="java.util.LinkedHashMap" mapping-class="org.datanucleus.store.rdbms.mapping.java.MapMapping"/>
    <mapping java-type="java.util.LinkedHashSet" mapping-class="org.datanucleus.store.rdbms.mapping.java.CollectionMapping"/>
    <mapping java-type="java.util.Map" mapping-class="org.datanucleus.store.rdbms.mapping.java.MapMapping"/>
    <mapping java-type="java.util.PriorityQueue" mapping-class="org.datanucleus.store.rdbms.mapping.java.CollectionMapping"/>
    <mapping java-type="java.util.Properties" mapping-class="org.datanucleus.store.rdbms.mapping.java.MapMapping"/>
    <mapping java-type="java.util.Queue" mapping-class="org.datanucleus.store.rdbms.mapping.java.CollectionMapping"/>
    <mapping java-type="java.util.Set" mapping-class="org.datanucleus.store.rdbms.mapping.java.CollectionMapping"/>
    <mapping java-type="java.util.SortedMap" mapping-class="org.datanucleus.store.rdbms.mapping.java.MapMapping"/>
    <mapping java-type="java.util.SortedSet" mapping-class="org.datanucleus.store.rdbms.mapping.java.CollectionMapping"/>
    <mapping java-type="java.util.Stack" mapping-class="org.datanucleus.store.rdbms.mapping.java.CollectionMapping"/>
    <mapping java-type="java.util.TreeMap" mapping-class="org.datanucleus.store.rdbms.mapping.java.MapMapping"/>
    <mapping java-type="java.util.TreeSet" mapping-class="org.datanucleus.store.rdbms.mapping.java.CollectionMapping"/>
    <mapping java-type="java.util.Vector" mapping-class="org.datanucleus.store.rdbms.mapping.java.CollectionMapping"/>

    <!-- DataNucleus types -->
    <mapping java-type="org.datanucleus.identity.DatastoreId" mapping-class="org.datanucleus.store.rdbms.mapping.java.DatastoreIdMapping"/>

    <mapping java-type="org.datanucleus.store.types.backed.ArrayList" mapping-class="org.datanucleus.store.rdbms.mapping.java.CollectionMapping"/>
    <mapping java-type="org.datanucleus.store.types.backed.Collection" mapping-class="org.datanucleus.store.rdbms.mapping.java.CollectionMapping"/>
    <mapping java-type="org.datanucleus.store.types.backed.HashMap" mapping-class="org.datanucleus.store.rdbms.mapping.java.MapMapping"/>
    <mapping java-type="org.datanucleus.store.types.backed.HashSet" mapping-class="org.datanucleus.store.rdbms.mapping.java.CollectionMapping"/>
    <mapping java-type="org.datanucleus.store.types.backed.Hashtable" mapping-class="org.datanucleus.store.rdbms.mapping.java.MapMapping"/>
    <mapping java-type="org.datanucleus.store.types.backed.LinkedList" mapping-class="org.datanucleus.store.rdbms.mapping.java.CollectionMapping"/>
    <mapping java-type="org.datanucleus.store.types.backed.List" mapping-class="org.datanucleus.store.rdbms.mapping.java.CollectionMapping"/>
    <mapping java-type="org.datanucleus.store.types.backed.LinkedHashSet" mapping-class="org.datanucleus.store.rdbms.mapping.java.CollectionMapping"/>
    <mapping java-type="org.datanucleus.store.types.backed.LinkedHashMap" mapping-class="org.datanucleus.store.rdbms.mapping.java.MapMapping"/>
    <mapping java-type="org.datanucleus.store.types.backed.Map" mapping-class="org.datanucleus.store.rdbms.mapping.java.MapMapping"/>
    <mapping java-type="org.datanucleus.store.types.backed.PriorityQueue" mapping-class="org.datanucleus.store.rdbms.mapping.java.CollectionMapping"/>
    <mapping java-type="org.datanucleus.store.types.backed.Properties" mapping-class="org.datanucleus.store.rdbms.mapping.java.MapMapping"/>
    <mapping java-type="org.datanucleus.store.types.backed.Queue" mapping-class="org.datanucleus.store.rdbms.mapping.java.CollectionMapping"/>
    <mapping java-type="org.datanucleus.store.types.backed.Set" mapping-class="org.datanucleus.store.rdbms.mapping.java.CollectionMapping"/>
    <mapping java-type="org.datanucleus.store.types.backed.SortedMap" mapping-class="org.datanucleus.store.rdbms.mapping.java.MapMapping"/>
    <mapping java-type="org.datanucleus.store.types.backed.SortedSet" mapping-class="org.datanucleus.store.rdbms.mapping.java.CollectionMapping"/>
    <mapping java-type="org.datanucleus.store.types.backed.Stack" mapping-class="org.datanucleus.store.rdbms.mapping.java.CollectionMapping"/>
    <mapping java-type="org.datanucleus.store.types.backed.TreeMap" mapping-class="org.datanucleus.store.rdbms.mapping.java.MapMapping"/>
    <mapping java-type="org.datanucleus.store.types.backed.TreeSet" mapping-class="org.datanucleus.store.rdbms.mapping.java.CollectionMapping"/>
    <mapping java-type="org.datanucleus.store.types.backed.Vector" mapping-class="org.datanucleus.store.rdbms.mapping.java.CollectionMapping"/>
  </extension>

  <!-- RDBMS Datastore Mapping -->
  <extension point="org.datanucleus.store.rdbms.datastore_mapping">
    <mapping java-type="java.lang.Boolean" jdbc-type="BIT" sql-type="BIT" default="true"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.BitRDBMSMapping">
      <excludes vendor-id="derby"/>
      <excludes vendor-id="firebird"/>
      <excludes vendor-id="h2"/>
      <excludes vendor-id="hsql"/>
      <excludes vendor-id="pointbase"/>
      <excludes vendor-id="sapdb"/>
    </mapping>
    <mapping java-type="java.lang.Boolean" jdbc-type="CHAR" sql-type="CHAR" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.CharRDBMSMapping"/>
    <mapping java-type="java.lang.Boolean" jdbc-type="BOOLEAN" sql-type="BOOLEAN" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.BooleanRDBMSMapping">
      <excludes vendor-id="h2"/>
    </mapping>
    <mapping java-type="java.lang.Boolean" jdbc-type="BOOLEAN" sql-type="BOOLEAN" default="true"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.BooleanRDBMSMapping">
      <includes vendor-id="h2"/>
    </mapping>
    <mapping java-type="java.lang.Boolean" jdbc-type="TINYINT" sql-type="TINYINT" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.TinyIntRDBMSMapping">
      <excludes vendor-id="db2"/>
      <excludes vendor-id="derby"/>
      <excludes vendor-id="sapdb"/>
      <excludes vendor-id="sqlserver"/>
      <excludes vendor-id="nuodb"/>
    </mapping>
    <mapping java-type="java.lang.Boolean" jdbc-type="SMALLINT" sql-type="SMALLINT" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.SmallIntRDBMSMapping"/>
    <mapping java-type="java.lang.Boolean" jdbc-type="CHAR" sql-type="CHAR" default="true"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.CharRDBMSMapping">
      <includes vendor-id="db2"/>
      <includes vendor-id="derby"/>
      <includes vendor-id="firebird"/>
      <includes vendor-id="pointbase"/>
      <includes vendor-id="sapdb"/>
    </mapping>
    <mapping java-type="java.lang.Boolean" jdbc-type="BOOLEAN" sql-type="BOOLEAN" default="true"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.CharRDBMSMapping">
      <includes vendor-id="hsql"/>
    </mapping>
    <mapping java-type="java.lang.Boolean" jdbc-type="NUMERIC" sql-type="NUMERIC" default="true"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.NumericRDBMSMapping">
      <includes vendor-id="oracle"/>
    </mapping>

    <mapping java-type="java.lang.Byte" jdbc-type="TINYINT" sql-type="TINYINT" default="true"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.TinyIntRDBMSMapping">
      <excludes vendor-id="db2"/>
      <excludes vendor-id="derby"/>
      <excludes vendor-id="sapdb"/>
      <excludes vendor-id="sqlserver"/>
      <excludes vendor-id="nuodb"/>
    </mapping>
    <mapping java-type="java.lang.Byte" jdbc-type="SMALLINT" sql-type="SMALLINT" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.SmallIntRDBMSMapping"/>
    <mapping java-type="java.lang.Byte" jdbc-type="NUMERIC" sql-type="NUMERIC" default="true"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.NumericRDBMSMapping">
      <includes vendor-id="oracle"/>
    </mapping>

    <mapping java-type="java.lang.Character" jdbc-type="CHAR" sql-type="CHAR" default="true"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.CharRDBMSMapping"/>
    <mapping java-type="java.lang.Character" jdbc-type="INTEGER" sql-type="INTEGER" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.IntegerRDBMSMapping">
      <excludes vendor-id="sqlserver"/>
    </mapping>
    <mapping java-type="java.lang.Character" jdbc-type="INTEGER" sql-type="INT" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.IntegerRDBMSMapping">
      <includes vendor-id="derby"/>
      <includes vendor-id="sapdb"/>
      <includes vendor-id="sqlserver"/>
      <includes vendor-id="sybase"/>
    </mapping>
    <mapping java-type="java.lang.Character" jdbc-type="NUMERIC" sql-type="NUMERIC" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.NumericRDBMSMapping">
      <includes vendor-id="oracle"/>
    </mapping>

    <mapping java-type="java.lang.Double" jdbc-type="DOUBLE" sql-type="DOUBLE" default="true"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.DoubleRDBMSMapping">
      <excludes vendor-id="informix"/>
      <excludes vendor-id="sqlserver"/>
    </mapping>
    <mapping java-type="java.lang.Double" jdbc-type="DOUBLE" sql-type="FLOAT" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.DoubleRDBMSMapping">
      <includes vendor-id="sqlserver"/>
    </mapping>
    <mapping java-type="java.lang.Double" jdbc-type="DECIMAL" sql-type="DECIMAL" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.DecimalRDBMSMapping">
      <excludes vendor-id="postgresql"/>
    </mapping>
    <mapping java-type="java.lang.Double" jdbc-type="FLOAT" sql-type="FLOAT" default="true"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.FloatRDBMSMapping">
      <includes vendor-id="informix"/>
      <includes vendor-id="oracle"/>
      <includes vendor-id="sqlserver"/>
    </mapping>
    <mapping java-type="java.lang.Double" jdbc-type="NUMERIC" sql-type="NUMBER" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.NumericRDBMSMapping">
      <includes vendor-id="oracle"/>
    </mapping>

    <mapping java-type="java.lang.Float" jdbc-type="FLOAT" sql-type="FLOAT" default="true"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.FloatRDBMSMapping">
      <excludes vendor-id="postgresql"/>
      <excludes vendor-id="sybase"/>
      <excludes vendor-id="db2"/>
    </mapping>
    <mapping java-type="java.lang.Float" jdbc-type="DOUBLE" sql-type="DOUBLE" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.DoubleRDBMSMapping">
      <excludes vendor-id="informix"/>
      <excludes vendor-id="sqlserver"/>
    </mapping>
    <mapping java-type="java.lang.Float" jdbc-type="REAL" sql-type="REAL" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.RealRDBMSMapping">
      <excludes vendor-id="db2"/>
    </mapping>
    <mapping java-type="java.lang.Float" jdbc-type="REAL" sql-type="REAL" default="true"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.RealRDBMSMapping">
      <includes vendor-id="db2"/>
    </mapping>
    <mapping java-type="java.lang.Float" jdbc-type="DECIMAL" sql-type="DECIMAL" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.DecimalRDBMSMapping">
      <excludes vendor-id="postgresql"/>
    </mapping>
    <mapping java-type="java.lang.Float" jdbc-type="DOUBLE" sql-type="DOUBLE" default="true"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.DoubleRDBMSMapping">
      <includes vendor-id="postgresql"/>
      <includes vendor-id="sybase"/>
    </mapping>
    <mapping java-type="java.lang.Float" jdbc-type="NUMERIC" sql-type="NUMBER" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.NumericRDBMSMapping">
      <includes vendor-id="oracle"/>
    </mapping>

    <mapping java-type="java.lang.Integer" jdbc-type="INTEGER" sql-type="INTEGER" default="true"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.IntegerRDBMSMapping">
      <excludes vendor-id="sqlserver"/>
    </mapping>
    <mapping java-type="java.lang.Integer" jdbc-type="BIGINT" sql-type="BIGINT" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.BigIntRDBMSMapping">
      <excludes vendor-id="sapdb"/>
      <excludes vendor-id="sybase"/>
    </mapping>
    <mapping java-type="java.lang.Integer" jdbc-type="NUMERIC" sql-type="NUMERIC" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.NumericRDBMSMapping">
      <excludes vendor-id="sapdb"/>
    </mapping>
    <mapping java-type="java.lang.Integer" jdbc-type="INTEGER" sql-type="INT" default="true"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.IntegerRDBMSMapping">
      <includes vendor-id="derby"/>
      <includes vendor-id="sapdb"/>
      <includes vendor-id="sqlserver"/>
      <includes vendor-id="sybase"/>
    </mapping>
    <mapping java-type="java.lang.Integer" jdbc-type="NUMERIC" sql-type="NUMERIC" default="true"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.NumericRDBMSMapping">
      <includes vendor-id="oracle"/>
    </mapping>
    <mapping java-type="java.lang.Integer" jdbc-type="NUMERIC" sql-type="NUMBER" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.NumericRDBMSMapping">
      <includes vendor-id="oracle"/>
    </mapping>
    <mapping java-type="java.lang.Integer" jdbc-type="TINYINT" sql-type="TINYINT" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.TinyIntRDBMSMapping">
      <excludes vendor-id="db2"/>
      <excludes vendor-id="derby"/>
      <excludes vendor-id="sapdb"/>
      <excludes vendor-id="sqlserver"/>
      <excludes vendor-id="nuodb"/>
    </mapping>
    <mapping java-type="java.lang.Integer" jdbc-type="SMALLINT" sql-type="SMALLINT" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.SmallIntRDBMSMapping"/>
    <mapping java-type="java.lang.Integer" jdbc-type="BIGINT" sql-type="BIGINT" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.BigIntRDBMSMapping">
      <excludes vendor-id="sapdb"/>
      <excludes vendor-id="sybase"/>
    </mapping>

    <mapping java-type="java.lang.Long" jdbc-type="BIGINT" sql-type="BIGINT" default="true"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.BigIntRDBMSMapping">
      <excludes vendor-id="sapdb"/>
      <excludes vendor-id="sybase"/>
    </mapping>
    <mapping java-type="java.lang.Long" jdbc-type="INTEGER" sql-type="INT" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.IntegerRDBMSMapping">
      <excludes vendor-id="sybase"/>
    </mapping>
    <mapping java-type="java.lang.Long" jdbc-type="NUMERIC" sql-type="NUMERIC" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.NumericRDBMSMapping">
      <excludes vendor-id="sapdb"/>
    </mapping>
    <mapping java-type="java.lang.Long" jdbc-type="DOUBLE" sql-type="DOUBLE" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.DoubleRDBMSMapping">
      <includes vendor-id="sapdb"/>
    </mapping>
    <mapping java-type="java.lang.Long" jdbc-type="DECIMAL" sql-type="NUMERIC" default="true"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.DecimalRDBMSMapping">
      <includes vendor-id="sapdb"/>
    </mapping>
    <mapping java-type="java.lang.Long" jdbc-type="INTEGER" sql-type="INT" default="true"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.IntegerRDBMSMapping">
      <includes vendor-id="sybase"/>
    </mapping>
    <mapping java-type="java.lang.Long" jdbc-type="NUMERIC" sql-type="NUMERIC" default="true"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.NumericRDBMSMapping">
      <includes vendor-id="oracle"/>
    </mapping>
    <mapping java-type="java.lang.Long" jdbc-type="NUMERIC" sql-type="NUMBER" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.NumericRDBMSMapping">
      <includes vendor-id="oracle"/>
    </mapping>
    <mapping java-type="java.lang.Long" jdbc-type="TINYINT" sql-type="TINYINT" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.TinyIntRDBMSMapping">
      <excludes vendor-id="db2"/>
      <excludes vendor-id="derby"/>
      <excludes vendor-id="sapdb"/>
      <excludes vendor-id="sqlserver"/>
      <excludes vendor-id="nuodb"/>
    </mapping>
    <mapping java-type="java.lang.Long" jdbc-type="SMALLINT" sql-type="SMALLINT" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.SmallIntRDBMSMapping"/>

    <mapping java-type="java.lang.Short" jdbc-type="SMALLINT" sql-type="SMALLINT" default="true"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.SmallIntRDBMSMapping"/>
    <mapping java-type="java.lang.Short" jdbc-type="INTEGER" sql-type="INTEGER" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.IntegerRDBMSMapping">
      <excludes vendor-id="sqlserver"/>
    </mapping>
    <mapping java-type="java.lang.Short" jdbc-type="INTEGER" sql-type="INT" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.IntegerRDBMSMapping">
      <includes vendor-id="derby"/>
      <includes vendor-id="sapdb"/>
      <includes vendor-id="sqlserver"/>
      <includes vendor-id="sybase"/>
    </mapping>
    <mapping java-type="java.lang.Short" jdbc-type="NUMERIC" sql-type="NUMERIC" default="true"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.NumericRDBMSMapping">
      <includes vendor-id="oracle"/>
    </mapping>
    <mapping java-type="java.lang.Short" jdbc-type="NUMERIC" sql-type="NUMBER" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.NumericRDBMSMapping">
      <includes vendor-id="oracle"/>
    </mapping>
    <mapping java-type="java.lang.Short" jdbc-type="TINYINT" sql-type="TINYINT" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.TinyIntRDBMSMapping">
      <excludes vendor-id="db2"/>
      <excludes vendor-id="derby"/>
      <excludes vendor-id="sapdb"/>
      <excludes vendor-id="sqlserver"/>
      <excludes vendor-id="nuodb"/>
    </mapping>

    <mapping java-type="java.lang.String" jdbc-type="VARCHAR" sql-type="VARCHAR" default="true"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.VarCharRDBMSMapping"/>
    <mapping java-type="java.lang.String" jdbc-type="CHAR" sql-type="CHAR" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.CharRDBMSMapping"/>
    <mapping java-type="java.lang.String" jdbc-type="BIGINT" sql-type="BIGINT" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.BigIntRDBMSMapping"/>
    <mapping java-type="java.lang.String" jdbc-type="LONGVARCHAR" sql-type="LONGVARCHAR" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.LongVarcharRDBMSMapping"/>
    <mapping java-type="java.lang.String" jdbc-type="CLOB" sql-type="CLOB" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.ClobRDBMSMapping">
      <excludes vendor-id="oracle"/>
    </mapping>
    <mapping java-type="java.lang.String" jdbc-type="CLOB" sql-type="CLOB" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.OracleClobRDBMSMapping">
      <includes vendor-id="oracle"/>
    </mapping>
    <mapping java-type="java.lang.String" jdbc-type="BLOB" sql-type="BLOB" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.BlobRDBMSMapping">
      <excludes vendor-id="oracle"/>
    </mapping>
    <mapping java-type="java.lang.String" jdbc-type="BLOB" sql-type="BLOB" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.OracleBlobRDBMSMapping">
      <includes vendor-id="oracle"/>
    </mapping>
    <mapping java-type="java.lang.String" jdbc-type="DATALINK" sql-type="DATALINK" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.DB2DatalinkRDBMSMapping">
      <includes vendor-id="db2"/>
    </mapping>
    <mapping java-type="java.lang.String" jdbc-type="CHAR" sql-type="UNIQUEIDENTIFIER" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.CharRDBMSMapping">
      <includes vendor-id="sqlserver"/>
    </mapping>
    <mapping java-type="java.lang.String" jdbc-type="SQLXML" sql-type="SQLXML" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.SqlXmlRDBMSMapping">
      <includes vendor-id="db2"/>
    </mapping>
    <mapping java-type="java.lang.String" jdbc-type="LONGVARCHAR" sql-type="LONGTEXT" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.LongVarcharRDBMSMapping">
      <includes vendor-id="mysql"/>
    </mapping>
    <mapping java-type="java.lang.String" jdbc-type="LONGVARCHAR" sql-type="MEDIUMTEXT" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.LongVarcharRDBMSMapping">
      <includes vendor-id="mysql"/>
    </mapping>
    <mapping java-type="java.lang.String" jdbc-type="LONGVARCHAR" sql-type="TEXT" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.LongVarcharRDBMSMapping">
      <includes vendor-id="mysql"/>
    </mapping>
    <mapping java-type="java.lang.String" jdbc-type="BLOB" sql-type="LONGBLOB" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.BlobRDBMSMapping">
      <includes vendor-id="mysql"/>
    </mapping>
    <mapping java-type="java.lang.String" jdbc-type="BLOB" sql-type="MEDIUMBLOB" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.BlobRDBMSMapping">
      <includes vendor-id="mysql"/>
    </mapping>
    <mapping java-type="java.lang.String" jdbc-type="XMLTYPE" sql-type="XMLTYPE" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.OracleXMLTypeRDBMSMapping">
      <includes vendor-id="oracle"/>
    </mapping>
    <mapping java-type="java.lang.String" jdbc-type="NVARCHAR" sql-type="NVARCHAR" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.NVarcharRDBMSMapping"/>
    <mapping java-type="java.lang.String" jdbc-type="NCHAR" sql-type="NCHAR" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.NCharRDBMSMapping"/>

    <mapping java-type="java.math.BigDecimal" jdbc-type="DECIMAL" sql-type="DECIMAL" default="true"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.DecimalRDBMSMapping">
      <excludes vendor-id="postgresql"/>
    </mapping>
    <mapping java-type="java.math.BigDecimal" jdbc-type="NUMERIC" sql-type="NUMERIC" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.NumericRDBMSMapping">
      <excludes vendor-id="sapdb"/>
    </mapping>
    <mapping java-type="java.math.BigDecimal" jdbc-type="NUMERIC" sql-type="NUMERIC" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.NumericRDBMSMapping">
      <includes vendor-id="postgresql"/>
    </mapping>
    <mapping java-type="java.math.BigDecimal" jdbc-type="NUMERIC" sql-type="NUMERIC" default="true"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.NumericRDBMSMapping">
      <includes vendor-id="oracle"/>
    </mapping>
    <mapping java-type="java.math.BigDecimal" jdbc-type="NUMERIC" sql-type="NUMBER" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.NumericRDBMSMapping">
      <includes vendor-id="oracle"/>
    </mapping>

    <mapping java-type="java.math.BigInteger" jdbc-type="NUMERIC" sql-type="NUMERIC" default="true"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.NumericRDBMSMapping">
      <excludes vendor-id="sapdb"/>
    </mapping>
    <mapping java-type="java.math.BigInteger" jdbc-type="DECIMAL" sql-type="NUMERIC" default="true"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.DecimalRDBMSMapping">
      <includes vendor-id="sapdb"/>
    </mapping>
    <mapping java-type="java.math.BigInteger" jdbc-type="NUMERIC" sql-type="NUMBER" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.NumericRDBMSMapping">
      <includes vendor-id="oracle"/>
    </mapping>

    <mapping java-type="java.sql.Date" jdbc-type="DATE" sql-type="DATE" default="true"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.DateRDBMSMapping"/>
    <mapping java-type="java.sql.Date" jdbc-type="TIMESTAMP" sql-type="TIMESTAMP" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.TimestampRDBMSMapping"/>
    <mapping java-type="java.sql.Date" jdbc-type="CHAR" sql-type="CHAR" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.CharRDBMSMapping"/>
    <mapping java-type="java.sql.Date" jdbc-type="VARCHAR" sql-type="VARCHAR" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.VarCharRDBMSMapping"/>
    <mapping java-type="java.sql.Date" jdbc-type="BIGINT" sql-type="BIGINT" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.BigIntRDBMSMapping">
      <excludes vendor-id="sapdb"/>
      <excludes vendor-id="sybase"/>
    </mapping>

    <mapping java-type="java.sql.Time" jdbc-type="TIME" sql-type="TIME" default="true"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.TimeRDBMSMapping"/>
    <mapping java-type="java.sql.Time" jdbc-type="TIMESTAMP" sql-type="TIMESTAMP" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.TimestampRDBMSMapping"/>
    <mapping java-type="java.sql.Time" jdbc-type="CHAR" sql-type="CHAR" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.CharRDBMSMapping"/>
    <mapping java-type="java.sql.Time" jdbc-type="VARCHAR" sql-type="VARCHAR" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.VarCharRDBMSMapping"/>
    <mapping java-type="java.sql.Time" jdbc-type="BIGINT" sql-type="BIGINT" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.BigIntRDBMSMapping">
      <excludes vendor-id="sapdb"/>
      <excludes vendor-id="sybase"/>
    </mapping>

    <mapping java-type="java.sql.Timestamp" jdbc-type="TIMESTAMP" sql-type="TIMESTAMP" default="true"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.TimestampRDBMSMapping"/>
    <mapping java-type="java.sql.Timestamp" jdbc-type="CHAR" sql-type="CHAR" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.CharRDBMSMapping"/>
    <mapping java-type="java.sql.Timestamp" jdbc-type="VARCHAR" sql-type="VARCHAR" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.VarCharRDBMSMapping"/>
    <mapping java-type="java.sql.Timestamp" jdbc-type="DATE" sql-type="DATE" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.DateRDBMSMapping"/>
    <mapping java-type="java.sql.Timestamp" jdbc-type="TIME" sql-type="TIME" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.TimeRDBMSMapping"/>

    <mapping java-type="java.util.Date" jdbc-type="TIMESTAMP" sql-type="TIMESTAMP" default="true"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.TimestampRDBMSMapping"/>
    <mapping java-type="java.util.Date" jdbc-type="DATE" sql-type="DATE" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.DateRDBMSMapping"/>
    <mapping java-type="java.util.Date" jdbc-type="CHAR" sql-type="CHAR" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.CharRDBMSMapping"/>
    <mapping java-type="java.util.Date" jdbc-type="VARCHAR" sql-type="VARCHAR" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.VarCharRDBMSMapping"/>
    <mapping java-type="java.util.Date" jdbc-type="BIGINT" sql-type="BIGINT" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.BigIntRDBMSMapping">
      <excludes vendor-id="sapdb"/>
      <excludes vendor-id="sybase"/>
    </mapping>
    <mapping java-type="java.util.Date" jdbc-type="TIME" sql-type="TIME" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.TimeRDBMSMapping"/>

    <mapping java-type="java.util.UUID" jdbc-type="OTHER" sql-type="UUID" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.OtherRDBMSMapping">
      <includes vendor-id="postgresql"/>
    </mapping>

    <mapping java-type="java.io.Serializable" jdbc-type="LONGVARBINARY" sql-type="LONGVARBINARY" default="true"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.LongVarBinaryRDBMSMapping">
    </mapping>
    <mapping java-type="java.io.Serializable" jdbc-type="BLOB" sql-type="BLOB" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.BlobRDBMSMapping">
      <excludes vendor-id="oracle"/>
    </mapping>
    <mapping java-type="java.io.Serializable" jdbc-type="BLOB" sql-type="BLOB" default="true"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.OracleBlobRDBMSMapping">
      <includes vendor-id="oracle"/>
    </mapping>
    <mapping java-type="java.io.Serializable" jdbc-type="VARBINARY" sql-type="VARBINARY" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.VarBinaryRDBMSMapping">
      <excludes vendor-id="timesten"/>
    </mapping>
    <mapping java-type="java.io.Serializable" jdbc-type="VARBINARY" sql-type="VARBINARY" default="true"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.TimesTenVarBinaryRDBMSMapping">
      <includes vendor-id="timesten"/>
    </mapping>
    <mapping java-type="java.io.File" jdbc-type="LONGVARBINARY" sql-type="LONGVARBINARY" default="true"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.BinaryStreamRDBMSMapping">
    </mapping>

    <mapping java-type="org.datanucleus.identity.DatastoreId" jdbc-type="BIGINT" sql-type="BIGINT" default="true"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.BigIntRDBMSMapping">
      <excludes vendor-id="sapdb"/>
      <excludes vendor-id="sybase"/>
    </mapping>
    <mapping java-type="org.datanucleus.identity.DatastoreId" jdbc-type="INTEGER" sql-type="INTEGER" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.IntegerRDBMSMapping">
      <excludes vendor-id="sqlserver"/>
    </mapping>
    <mapping java-type="org.datanucleus.identity.DatastoreId" jdbc-type="NUMERIC" sql-type="NUMERIC" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.NumericRDBMSMapping">
      <excludes vendor-id="sapdb"/>
    </mapping>
    <mapping java-type="org.datanucleus.identity.DatastoreId" jdbc-type="CHAR" sql-type="CHAR" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.CharRDBMSMapping"/>
    <mapping java-type="org.datanucleus.identity.DatastoreId" jdbc-type="VARCHAR" sql-type="VARCHAR" default="false"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.VarCharRDBMSMapping"/>
    <mapping java-type="org.datanucleus.identity.DatastoreId" jdbc-type="INTEGER" sql-type="INT" default="true"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.IntegerRDBMSMapping">
      <includes vendor-id="sapdb"/>
      <includes vendor-id="sqlserver"/>
      <includes vendor-id="sybase"/>
    </mapping>
    <mapping java-type="org.datanucleus.identity.DatastoreId" jdbc-type="NUMERIC" sql-type="NUMERIC" default="true"
             rdbms-mapping-class="org.datanucleus.store.rdbms.mapping.datastore.NumericRDBMSMapping">
      <includes vendor-id="oracle"/>
    </mapping>
  </extension>

  <!-- IDENTIFIER FACTORIES. TODO Adopt NamingFactory -->
  <extension point="org.datanucleus.store.rdbms.identifierfactory">
    <identifierfactory name="datanucleus2" class-name="org.datanucleus.store.rdbms.identifier.DN2IdentifierFactory"/>
    <identifierfactory name="jpa" class-name="org.datanucleus.store.rdbms.identifier.JPAIdentifierFactory"/>
    <identifierfactory name="datanucleus1" class-name="org.datanucleus.store.rdbms.identifier.DNIdentifierFactory"/>
    <identifierfactory name="jpox" class-name="org.datanucleus.store.rdbms.identifier.JPOXIdentifierFactory"/>
  </extension>

  <!-- SQL EXPRESSIONS -->
  <extension point="org.datanucleus.store.rdbms.sql_expression">
    <sql-expression mapping-class="org.datanucleus.store.rdbms.mapping.java.ArrayMapping"
                    literal-class="org.datanucleus.store.rdbms.sql.expression.ArrayLiteral"
                    expression-class="org.datanucleus.store.rdbms.sql.expression.ArrayExpression"/>
    <sql-expression mapping-class="org.datanucleus.store.rdbms.mapping.java.OracleArrayMapping"
                    literal-class="org.datanucleus.store.rdbms.sql.expression.ArrayLiteral"
                    expression-class="org.datanucleus.store.rdbms.sql.expression.ArrayExpression"/>
    <sql-expression mapping-class="org.datanucleus.store.rdbms.mapping.java.BigDecimalMapping"
                    literal-class="org.datanucleus.store.rdbms.sql.expression.FloatingPointLiteral"
                    expression-class="org.datanucleus.store.rdbms.sql.expression.NumericExpression"/>
    <sql-expression mapping-class="org.datanucleus.store.rdbms.mapping.java.BigIntegerMapping"
                    literal-class="org.datanucleus.store.rdbms.sql.expression.IntegerLiteral"
                    expression-class="org.datanucleus.store.rdbms.sql.expression.NumericExpression"/>
    <sql-expression mapping-class="org.datanucleus.store.rdbms.mapping.java.BooleanMapping"
                    literal-class="org.datanucleus.store.rdbms.sql.expression.BooleanLiteral"
                    expression-class="org.datanucleus.store.rdbms.sql.expression.BooleanExpression"/>
    <sql-expression mapping-class="org.datanucleus.store.rdbms.mapping.java.ByteMapping"
                    literal-class="org.datanucleus.store.rdbms.sql.expression.ByteLiteral"
                    expression-class="org.datanucleus.store.rdbms.sql.expression.ByteExpression"/>
    <sql-expression mapping-class="org.datanucleus.store.rdbms.mapping.java.CharacterMapping"
                    literal-class="org.datanucleus.store.rdbms.sql.expression.CharacterLiteral"
                    expression-class="org.datanucleus.store.rdbms.sql.expression.CharacterExpression"/>
    <sql-expression mapping-class="org.datanucleus.store.rdbms.mapping.java.CollectionMapping"
                    literal-class="org.datanucleus.store.rdbms.sql.expression.CollectionLiteral"
                    expression-class="org.datanucleus.store.rdbms.sql.expression.CollectionExpression"/>
    <sql-expression mapping-class="org.datanucleus.store.rdbms.mapping.java.OracleCollectionMapping"
                    literal-class="org.datanucleus.store.rdbms.sql.expression.CollectionLiteral"
                    expression-class="org.datanucleus.store.rdbms.sql.expression.CollectionExpression"/>
    <sql-expression mapping-class="org.datanucleus.store.rdbms.mapping.java.DateMapping"
                    literal-class="org.datanucleus.store.rdbms.sql.expression.TemporalLiteral"
                    expression-class="org.datanucleus.store.rdbms.sql.expression.TemporalExpression"/>
    <sql-expression mapping-class="org.datanucleus.store.rdbms.mapping.java.DiscriminatorLongMapping"
                    literal-class="org.datanucleus.store.rdbms.sql.expression.IntegerLiteral"
                    expression-class="org.datanucleus.store.rdbms.sql.expression.NumericExpression"/>
    <sql-expression mapping-class="org.datanucleus.store.rdbms.mapping.java.DiscriminatorStringMapping"
                    literal-class="org.datanucleus.store.rdbms.sql.expression.StringLiteral"
                    expression-class="org.datanucleus.store.rdbms.sql.expression.StringExpression"/>
    <sql-expression mapping-class="org.datanucleus.store.rdbms.mapping.java.DoubleMapping"
                    literal-class="org.datanucleus.store.rdbms.sql.expression.FloatingPointLiteral"
                    expression-class="org.datanucleus.store.rdbms.sql.expression.NumericExpression"/>
    <sql-expression mapping-class="org.datanucleus.store.rdbms.mapping.java.EnumMapping"
                    literal-class="org.datanucleus.store.rdbms.sql.expression.EnumLiteral"
                    expression-class="org.datanucleus.store.rdbms.sql.expression.EnumExpression"/>
    <sql-expression mapping-class="org.datanucleus.store.rdbms.mapping.java.FloatMapping"
                    literal-class="org.datanucleus.store.rdbms.sql.expression.FloatingPointLiteral"
                    expression-class="org.datanucleus.store.rdbms.sql.expression.NumericExpression"/>
    <sql-expression mapping-class="org.datanucleus.store.rdbms.mapping.java.GregorianCalendarMapping"
                    literal-class="org.datanucleus.store.rdbms.sql.expression.TemporalLiteral"
                    expression-class="org.datanucleus.store.rdbms.sql.expression.TemporalExpression"/>
    <sql-expression mapping-class="org.datanucleus.store.rdbms.mapping.java.IndexMapping"
                    literal-class="org.datanucleus.store.rdbms.sql.expression.IntegerLiteral"
                    expression-class="org.datanucleus.store.rdbms.sql.expression.NumericExpression"/>
    <sql-expression mapping-class="org.datanucleus.store.rdbms.mapping.java.IntegerMapping"
                    literal-class="org.datanucleus.store.rdbms.sql.expression.IntegerLiteral"
                    expression-class="org.datanucleus.store.rdbms.sql.expression.NumericExpression"/>
    <sql-expression mapping-class="org.datanucleus.store.rdbms.mapping.java.LongMapping"
                    literal-class="org.datanucleus.store.rdbms.sql.expression.IntegerLiteral"
                    expression-class="org.datanucleus.store.rdbms.sql.expression.NumericExpression"/>
    <sql-expression mapping-class="org.datanucleus.store.rdbms.mapping.java.MapMapping"
                    literal-class="org.datanucleus.store.rdbms.sql.expression.MapLiteral"
                    expression-class="org.datanucleus.store.rdbms.sql.expression.MapExpression"/>
    <sql-expression mapping-class="org.datanucleus.store.rdbms.mapping.java.OracleMapMapping"
                    literal-class="org.datanucleus.store.rdbms.sql.expression.MapLiteral"
                    expression-class="org.datanucleus.store.rdbms.sql.expression.MapExpression"/>
    <sql-expression mapping-class="org.datanucleus.store.rdbms.mapping.java.NumberMapping"
                    literal-class="org.datanucleus.store.rdbms.sql.expression.FloatingPointLiteral"
                    expression-class="org.datanucleus.store.rdbms.sql.expression.NumericExpression"/>
    <sql-expression mapping-class="org.datanucleus.store.rdbms.mapping.java.DatastoreIdMapping"
                    literal-class="org.datanucleus.store.rdbms.sql.expression.ObjectLiteral"
                    expression-class="org.datanucleus.store.rdbms.sql.expression.ObjectExpression"/>
    <sql-expression mapping-class="org.datanucleus.store.rdbms.mapping.java.PersistableMapping"
                    literal-class="org.datanucleus.store.rdbms.sql.expression.ObjectLiteral"
                    expression-class="org.datanucleus.store.rdbms.sql.expression.ObjectExpression"/>
    <sql-expression mapping-class="org.datanucleus.store.rdbms.mapping.java.PersistableIdMapping"
                    literal-class="org.datanucleus.store.rdbms.sql.expression.ObjectLiteral"
                    expression-class="org.datanucleus.store.rdbms.sql.expression.ObjectExpression"/>
    <sql-expression mapping-class="org.datanucleus.store.rdbms.mapping.java.ReferenceIdMapping"
                    literal-class="org.datanucleus.store.rdbms.sql.expression.ObjectLiteral"
                    expression-class="org.datanucleus.store.rdbms.sql.expression.ObjectExpression"/>
    <sql-expression mapping-class="org.datanucleus.store.rdbms.mapping.java.ObjectMapping"
                    literal-class="org.datanucleus.store.rdbms.sql.expression.ObjectLiteral"
                    expression-class="org.datanucleus.store.rdbms.sql.expression.ObjectExpression"/>
    <sql-expression mapping-class="org.datanucleus.store.rdbms.mapping.java.EmbeddedPCMapping"
                    literal-class="org.datanucleus.store.rdbms.sql.expression.ObjectLiteral"
                    expression-class="org.datanucleus.store.rdbms.sql.expression.ObjectExpression"/>
    <sql-expression mapping-class="org.datanucleus.store.rdbms.mapping.java.EmbeddedElementPCMapping"
                    literal-class="org.datanucleus.store.rdbms.sql.expression.ObjectLiteral"
                    expression-class="org.datanucleus.store.rdbms.sql.expression.ObjectExpression"/>
    <sql-expression mapping-class="org.datanucleus.store.rdbms.mapping.java.EmbeddedKeyPCMapping"
                    literal-class="org.datanucleus.store.rdbms.sql.expression.ObjectLiteral"
                    expression-class="org.datanucleus.store.rdbms.sql.expression.ObjectExpression"/>
    <sql-expression mapping-class="org.datanucleus.store.rdbms.mapping.java.EmbeddedValuePCMapping"
                    literal-class="org.datanucleus.store.rdbms.sql.expression.ObjectLiteral"
                    expression-class="org.datanucleus.store.rdbms.sql.expression.ObjectExpression"/>
    <sql-expression mapping-class="org.datanucleus.store.rdbms.mapping.java.ReferenceMapping"
                    literal-class="org.datanucleus.store.rdbms.sql.expression.ObjectLiteral"
                    expression-class="org.datanucleus.store.rdbms.sql.expression.ObjectExpression"/>
    <sql-expression mapping-class="org.datanucleus.store.rdbms.mapping.java.InterfaceMapping"
                    literal-class="org.datanucleus.store.rdbms.sql.expression.ObjectLiteral"
                    expression-class="org.datanucleus.store.rdbms.sql.expression.ObjectExpression"/>
    <sql-expression mapping-class="org.datanucleus.store.rdbms.mapping.java.ShortMapping"
                    literal-class="org.datanucleus.store.rdbms.sql.expression.IntegerLiteral"
                    expression-class="org.datanucleus.store.rdbms.sql.expression.NumericExpression"/>
    <sql-expression mapping-class="org.datanucleus.store.rdbms.mapping.java.SqlDateMapping"
                    literal-class="org.datanucleus.store.rdbms.sql.expression.TemporalLiteral"
                    expression-class="org.datanucleus.store.rdbms.sql.expression.TemporalExpression"/>
    <sql-expression mapping-class="org.datanucleus.store.rdbms.mapping.java.SqlTimeMapping"
                    literal-class="org.datanucleus.store.rdbms.sql.expression.TemporalLiteral"
                    expression-class="org.datanucleus.store.rdbms.sql.expression.TemporalExpression"/>
    <sql-expression mapping-class="org.datanucleus.store.rdbms.mapping.java.SqlTimestampMapping"
                    literal-class="org.datanucleus.store.rdbms.sql.expression.TemporalLiteral"
                    expression-class="org.datanucleus.store.rdbms.sql.expression.TemporalExpression"/>
    <sql-expression mapping-class="org.datanucleus.store.rdbms.mapping.java.StringMapping"
                    literal-class="org.datanucleus.store.rdbms.sql.expression.StringLiteral"
                    expression-class="org.datanucleus.store.rdbms.sql.expression.StringExpression"/>
    <sql-expression mapping-class="org.datanucleus.store.rdbms.mapping.java.OracleStringLobMapping"
                    literal-class="org.datanucleus.store.rdbms.sql.expression.StringLiteral"
                    expression-class="org.datanucleus.store.rdbms.sql.expression.StringExpression"/>
    <sql-expression mapping-class="org.datanucleus.store.rdbms.mapping.java.VersionLongMapping"
                    literal-class="org.datanucleus.store.rdbms.sql.expression.IntegerLiteral"
                    expression-class="org.datanucleus.store.rdbms.sql.expression.NumericExpression"/>
    <sql-expression mapping-class="org.datanucleus.store.rdbms.mapping.java.VersionTimestampMapping"
                    literal-class="org.datanucleus.store.rdbms.sql.expression.TemporalLiteral"
                    expression-class="org.datanucleus.store.rdbms.sql.expression.TemporalExpression"/>
    <sql-expression mapping-class="org.datanucleus.store.rdbms.mapping.java.ClassMapping"
                    literal-class="org.datanucleus.store.rdbms.sql.expression.StringLiteral"
                    expression-class="org.datanucleus.store.rdbms.sql.expression.StringExpression"/>

    <sql-expression mapping-class="org.datanucleus.store.rdbms.mapping.java.UUIDMapping"
                    literal-class="org.datanucleus.store.rdbms.sql.expression.StringLiteral"
                    expression-class="org.datanucleus.store.rdbms.sql.expression.StringExpression"/>

    <sql-expression mapping-class="org.datanucleus.store.rdbms.mapping.java.TypeConverterMapping"
                    literal-class="org.datanucleus.store.rdbms.sql.expression.TypeConverterLiteral"
                    expression-class="org.datanucleus.store.rdbms.sql.expression.TypeConverterExpression"/>
    <sql-expression mapping-class="org.datanucleus.store.rdbms.mapping.java.TypeConverterMultiMapping"
                    literal-class="org.datanucleus.store.rdbms.sql.expression.TypeConverterMultiLiteral"
                    expression-class="org.datanucleus.store.rdbms.sql.expression.TypeConverterMultiExpression"/>
  </extension>

  <!-- SQL METHODS -->
  <extension point="org.datanucleus.store.rdbms.sql_method">
    <sql-method method="abs" evaluator="org.datanucleus.store.rdbms.sql.method.AbsFunction"/>
    <sql-method method="acos" evaluator="org.datanucleus.store.rdbms.sql.method.AcosFunction"/>
    <sql-method method="asin" evaluator="org.datanucleus.store.rdbms.sql.method.AsinFunction"/>
    <sql-method method="atan" evaluator="org.datanucleus.store.rdbms.sql.method.AtanFunction"/>
    <sql-method method="avg" evaluator="org.datanucleus.store.rdbms.sql.method.AvgFunction"/>
    <sql-method method="avg" evaluator="org.datanucleus.store.rdbms.sql.method.AvgWithCastFunction" datastore="h2"/>
    <sql-method method="avg" evaluator="org.datanucleus.store.rdbms.sql.method.AvgWithCastFunction" datastore="hsql"/>
    <sql-method method="avg" evaluator="org.datanucleus.store.rdbms.sql.method.AvgWithCastFunction" datastore="derby"/>
    <sql-method method="ceil" evaluator="org.datanucleus.store.rdbms.sql.method.CeilFunction"/>
    <sql-method method="cos" evaluator="org.datanucleus.store.rdbms.sql.method.CosFunction"/>
    <sql-method method="count" evaluator="org.datanucleus.store.rdbms.sql.method.CountFunction"/>
    <sql-method method="exp" evaluator="org.datanucleus.store.rdbms.sql.method.ExpFunction"/>
    <sql-method method="floor" evaluator="org.datanucleus.store.rdbms.sql.method.FloorFunction"/>
    <sql-method method="log" evaluator="org.datanucleus.store.rdbms.sql.method.LogFunction"/>
    <sql-method method="log" evaluator="org.datanucleus.store.rdbms.sql.method.LogFunction2" datastore="postgresql"/>
    <sql-method method="max" evaluator="org.datanucleus.store.rdbms.sql.method.MaxFunction"/>
    <sql-method method="min" evaluator="org.datanucleus.store.rdbms.sql.method.MinFunction"/>
    <sql-method method="sin" evaluator="org.datanucleus.store.rdbms.sql.method.SinFunction"/>
    <sql-method method="sqrt" evaluator="org.datanucleus.store.rdbms.sql.method.SqrtFunction"/>
    <sql-method method="sum" evaluator="org.datanucleus.store.rdbms.sql.method.SumFunction"/>
    <sql-method method="tan" evaluator="org.datanucleus.store.rdbms.sql.method.TanFunction"/>
    <sql-method method="degrees" evaluator="org.datanucleus.store.rdbms.sql.method.DegreesFunction"/>
    <sql-method method="radians" evaluator="org.datanucleus.store.rdbms.sql.method.RadiansFunction"/>
    <sql-method method="ABS" evaluator="org.datanucleus.store.rdbms.sql.method.AbsFunction"/>
    <sql-method method="ACOS" evaluator="org.datanucleus.store.rdbms.sql.method.AcosFunction"/>
    <sql-method method="ASIN" evaluator="org.datanucleus.store.rdbms.sql.method.AsinFunction"/>
    <sql-method method="ATAN" evaluator="org.datanucleus.store.rdbms.sql.method.AtanFunction"/>
    <sql-method method="AVG" evaluator="org.datanucleus.store.rdbms.sql.method.AvgFunction"/>
    <sql-method method="AVG" evaluator="org.datanucleus.store.rdbms.sql.method.AvgWithCastFunction" datastore="h2"/>
    <sql-method method="AVG" evaluator="org.datanucleus.store.rdbms.sql.method.AvgWithCastFunction" datastore="hsql"/>
    <sql-method method="AVG" evaluator="org.datanucleus.store.rdbms.sql.method.AvgWithCastFunction" datastore="derby"/>
    <sql-method method="CEIL" evaluator="org.datanucleus.store.rdbms.sql.method.CeilFunction"/>
    <sql-method method="COS" evaluator="org.datanucleus.store.rdbms.sql.method.CosFunction"/>
    <sql-method method="COUNT" evaluator="org.datanucleus.store.rdbms.sql.method.CountFunction"/>
    <sql-method method="COUNTSTAR" evaluator="org.datanucleus.store.rdbms.sql.method.CountStarFunction"/>
    <sql-method method="EXP" evaluator="org.datanucleus.store.rdbms.sql.method.ExpFunction"/>
    <sql-method method="FLOOR" evaluator="org.datanucleus.store.rdbms.sql.method.FloorFunction"/>
    <sql-method method="LOG" evaluator="org.datanucleus.store.rdbms.sql.method.LogFunction"/>
    <sql-method method="MAX" evaluator="org.datanucleus.store.rdbms.sql.method.MaxFunction"/>
    <sql-method method="MIN" evaluator="org.datanucleus.store.rdbms.sql.method.MinFunction"/>
    <sql-method method="SIN" evaluator="org.datanucleus.store.rdbms.sql.method.SinFunction"/>
    <sql-method method="SQRT" evaluator="org.datanucleus.store.rdbms.sql.method.SqrtFunction"/>
    <sql-method method="SUM" evaluator="org.datanucleus.store.rdbms.sql.method.SumFunction"/>
    <sql-method method="TAN" evaluator="org.datanucleus.store.rdbms.sql.method.TanFunction"/>
    <sql-method method="RADIANS" evaluator="org.datanucleus.store.rdbms.sql.method.RadiansFunction"/>
    <sql-method method="DEGREES" evaluator="org.datanucleus.store.rdbms.sql.method.DegreesFunction"/>
    <sql-method method="COALESCE" evaluator="org.datanucleus.store.rdbms.sql.method.CoalesceFunction"/>
    <sql-method method="NULLIF" evaluator="org.datanucleus.store.rdbms.sql.method.NullIfFunction"/>
    <sql-method method="INDEX" evaluator="org.datanucleus.store.rdbms.sql.method.IndexFunction"/>
    <sql-method method="CURRENT_DATE" evaluator="org.datanucleus.store.rdbms.sql.method.CurrentDateFunction"/>
    <sql-method method="CURRENT_TIME" evaluator="org.datanucleus.store.rdbms.sql.method.CurrentTimeFunction"/>
    <sql-method method="CURRENT_TIMESTAMP" evaluator="org.datanucleus.store.rdbms.sql.method.CurrentTimestampFunction"/>
    <sql-method method="Math.abs" evaluator="org.datanucleus.store.rdbms.sql.method.MathAbsMethod"/>
    <sql-method method="Math.acos" evaluator="org.datanucleus.store.rdbms.sql.method.MathAcosMethod"/>
    <sql-method method="Math.asin" evaluator="org.datanucleus.store.rdbms.sql.method.MathAsinMethod"/>
    <sql-method method="Math.atan" evaluator="org.datanucleus.store.rdbms.sql.method.MathAtanMethod"/>
    <sql-method method="Math.ceil" evaluator="org.datanucleus.store.rdbms.sql.method.MathCeilMethod"/>
    <sql-method method="Math.cos" evaluator="org.datanucleus.store.rdbms.sql.method.MathCosMethod"/>
    <sql-method method="Math.exp" evaluator="org.datanucleus.store.rdbms.sql.method.MathExpMethod"/>
    <sql-method method="Math.floor" evaluator="org.datanucleus.store.rdbms.sql.method.MathFloorMethod"/>
    <sql-method method="Math.log" evaluator="org.datanucleus.store.rdbms.sql.method.MathLogMethod"/>
    <sql-method method="Math.sin" evaluator="org.datanucleus.store.rdbms.sql.method.MathSinMethod"/>
    <sql-method method="Math.sqrt" evaluator="org.datanucleus.store.rdbms.sql.method.MathSqrtMethod"/>
    <sql-method method="Math.tan" evaluator="org.datanucleus.store.rdbms.sql.method.MathTanMethod"/>
    <sql-method method="Math.toRadians" evaluator="org.datanucleus.store.rdbms.sql.method.MathToRadiansMethod"/>
    <sql-method method="Math.toDegrees" evaluator="org.datanucleus.store.rdbms.sql.method.MathToDegreesMethod"/>
    <sql-method method="JDOHelper.getObjectId"
                evaluator="org.datanucleus.store.rdbms.sql.method.JDOHelperGetObjectIdMethod"/>
    <sql-method method="JDOHelper.getVersion"
                evaluator="org.datanucleus.store.rdbms.sql.method.JDOHelperGetVersionMethod"/>
    <sql-method method="SQL_cube" datastore="db2"
                evaluator="org.datanucleus.store.rdbms.sql.method.SQLCubeFunction"/>
    <sql-method method="SQL_cube" datastore="oracle"
                evaluator="org.datanucleus.store.rdbms.sql.method.SQLCubeFunction"/>
    <sql-method method="SQL_cube" datastore="sqlserver"
                evaluator="org.datanucleus.store.rdbms.sql.method.SQLCubeFunction"/>
    <sql-method method="SQL_rollup" datastore="db2"
                evaluator="org.datanucleus.store.rdbms.sql.method.SQLRollupFunction"/>
    <sql-method method="SQL_rollup" datastore="oracle"
                evaluator="org.datanucleus.store.rdbms.sql.method.SQLRollupFunction"/>
    <sql-method method="SQL_rollup" datastore="sqlserver"
                evaluator="org.datanucleus.store.rdbms.sql.method.SQLRollupFunction"/>
    <sql-method method="SQL_boolean" evaluator="org.datanucleus.store.rdbms.sql.method.SQLBooleanMethod"/>
    <sql-method method="SQL_numeric" evaluator="org.datanucleus.store.rdbms.sql.method.SQLNumericMethod"/>
    <sql-method method="SQL_function" evaluator="org.datanucleus.store.rdbms.sql.method.SQLFunctionMethod"/>

    <sql-method class="java.lang.Character" method="toUpperCase"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringToUpperMethod"/>
    <sql-method class="java.lang.CHaracter" method="toLowerCase"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringToLowerMethod"/>
    <sql-method class="java.lang.Enum" method="ordinal"
                evaluator="org.datanucleus.store.rdbms.sql.method.EnumOrdinalMethod"/>
    <sql-method class="java.lang.Enum" method="toString"
                evaluator="org.datanucleus.store.rdbms.sql.method.EnumToStringMethod"/>
    <sql-method class="java.lang.Object" method="getClass"
                evaluator="org.datanucleus.store.rdbms.sql.method.ObjectGetClassMethod"/>
    <sql-method class="java.lang.String" method="charAt"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringCharAtMethod"/>
    <sql-method class="java.lang.String" method="concat" datastore="sqlserver"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringConcat2Method"/>
    <sql-method class="java.lang.String" method="concat" datastore="mysql"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringConcat2Method"/>
    <sql-method class="java.lang.String" method="concat" datastore="db2"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringConcat2Method"/>
    <sql-method class="java.lang.String" method="concat" datastore="derby"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringConcat1Method"/>
    <sql-method class="java.lang.String" method="concat" datastore="oracle"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringConcat1Method"/>
    <sql-method class="java.lang.String" method="concat" datastore="postgresql"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringConcat1Method"/>
    <sql-method class="java.lang.String" method="endsWith"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringEndsWithMethod"/>
    <sql-method class="java.lang.String" method="equals"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringEqualsMethod"/>
    <sql-method class="java.lang.String" method="equalsIgnoreCase"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringEqualsIgnoreCaseMethod"/>
    <sql-method class="java.lang.String" method="indexOf"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringIndexOfMethod"/>
    <sql-method class="java.lang.String" method="indexOf" datastore="oracle"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringIndexOf2Method"/>
    <sql-method class="java.lang.String" method="indexOf" datastore="db2"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringIndexOf3Method"/>
    <sql-method class="java.lang.String" method="indexOf" datastore="sqlserver"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringIndexOf4Method"/>
    <sql-method class="java.lang.String" method="indexOf" datastore="postgresql"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringIndexOf5Method"/>
    <sql-method class="java.lang.String" method="indexOf" datastore="sybase"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringIndexOf4Method"/>
    <sql-method class="java.lang.String" method="indexOf" datastore="sqlite"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringIndexOf2Method"/>
    <sql-method class="java.lang.String" method="length"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringLengthMethod"/>
    <sql-method class="java.lang.String" method="length" datastore="derby"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringLength3Method"/>
    <sql-method class="java.lang.String" method="length" datastore="db2"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringLength3Method"/>
    <sql-method class="java.lang.String" method="length" datastore="mckoi"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringLength3Method"/>
    <sql-method class="java.lang.String" method="length" datastore="firebird"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringLength2Method"/>
    <sql-method class="java.lang.String" method="length" datastore="oracle"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringLength3Method"/>
    <sql-method class="java.lang.String" method="length" datastore="sapdb"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringLength3Method"/>
    <sql-method class="java.lang.String" method="length" datastore="sqlite"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringLength3Method"/>
    <sql-method class="java.lang.String" method="matches"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringMatchesMethod"/>
    <sql-method class="java.lang.String" method="matches" datastore="derby"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringMatchesDerbyMethod"/>
    <sql-method class="java.lang.String" method="replaceAll"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringReplaceAllMethod"/>

    <sql-method class="java.lang.String" method="similarTo" datastore="postgresql"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringSimilarPostgresqlMethod"/>

    <sql-method class="java.lang.String" method="startsWith"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringStartsWithMethod"/>
    <sql-method class="java.lang.String" method="startsWith" datastore="sqlserver"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringStartsWith2Method"/>
    <sql-method class="java.lang.String" method="startsWith" datastore="mysql"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringStartsWith3Method"/>
    <sql-method class="java.lang.String" method="startsWith" datastore="hsql"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringStartsWith3Method"/>
    <sql-method class="java.lang.String" method="startsWith" datastore="derby"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringStartsWith3Method"/>
    <sql-method class="java.lang.String" method="substring"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringSubstringMethod"/>
    <sql-method class="java.lang.String" method="substring" datastore="mckoi"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringSubstring2Method"/>
    <sql-method class="java.lang.String" method="substring" datastore="sqlserver"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringSubstring4Method"/>
    <sql-method class="java.lang.String" method="substring" datastore="sybase"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringSubstring2Method"/>
    <sql-method class="java.lang.String" method="substring" datastore="sqlite"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringSubstring3Method"/>
    <sql-method class="java.lang.String" method="substring" datastore="db2"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringSubstring3Method"/>
    <sql-method class="java.lang.String" method="substring" datastore="derby"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringSubstring3Method"/>
    <sql-method class="java.lang.String" method="substring" datastore="informix"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringSubstring3Method"/>
    <sql-method class="java.lang.String" method="substring" datastore="oracle"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringSubstring3Method"/>
    <sql-method class="java.lang.String" method="substring" datastore="sapdb"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringSubstring3Method"/>
    <sql-method class="java.lang.String" method="substring" datastore="postgresql"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringSubstring5Method"/>
    <sql-method class="java.lang.String" method="toUpperCase"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringToUpperMethod"/>
    <sql-method class="java.lang.String" method="toLowerCase"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringToLowerMethod"/>
    <sql-method class="java.lang.String" method="translate"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringTranslateMethod" datastore="postgresql"/>
    <sql-method class="java.lang.String" method="translate"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringTranslateMethod" datastore="oracle"/>
    <sql-method class="java.lang.String" method="translate"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringTranslateMethod" datastore="db2"/>

    <sql-method class="java.lang.String" method="trim"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringTrimMethod"/>
    <sql-method class="java.lang.String" method="trim" datastore="derby"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringTrim2Method"/>
    <sql-method class="java.lang.String" method="trim" datastore="db2"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringTrim2Method"/>
    <sql-method class="java.lang.String" method="trim" datastore="hsql"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringTrim2Method"/>
    <sql-method class="java.lang.String" method="trim" datastore="sqlserver"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringTrim2Method"/>
    <sql-method class="java.lang.String" method="trim" datastore="mysql"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringTrim3Method"/>
    <sql-method class="java.lang.String" method="trim" datastore="postgresql"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringTrim3Method"/>

    <sql-method class="java.lang.String" method="trimLeft"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringTrimLeftMethod"/>
    <sql-method class="java.lang.String" method="trimLeft" datastore="mysql"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringTrimLeft3Method"/>
    <sql-method class="java.lang.String" method="trimLeft" datastore="postgresql"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringTrimLeft3Method"/>

    <sql-method class="java.lang.String" method="trimRight"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringTrimRightMethod"/>
    <sql-method class="java.lang.String" method="trimRight" datastore="mysql"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringTrimRight3Method"/>
    <sql-method class="java.lang.String" method="trimRight" datastore="postgresql"
                evaluator="org.datanucleus.store.rdbms.sql.method.StringTrimRight3Method"/>

    <sql-method class="java.util.Collection" method="contains"
                evaluator="org.datanucleus.store.rdbms.sql.method.CollectionContainsMethod"/>
    <sql-method class="java.util.Collection" method="isEmpty"
                evaluator="org.datanucleus.store.rdbms.sql.method.CollectionIsEmptyMethod"/>
    <sql-method class="java.util.Collection" method="size"
                evaluator="org.datanucleus.store.rdbms.sql.method.CollectionSizeMethod"/>
    <sql-method class="java.util.Collection" method="get"
                evaluator="org.datanucleus.store.rdbms.sql.method.ListGetMethod"/>

    <sql-method class="java.util.Date" method="getDay"
                evaluator="org.datanucleus.store.rdbms.sql.method.DateGetDayMethod"/>
    <sql-method class="java.util.Date" method="getDay" datastore="oracle"
                evaluator="org.datanucleus.store.rdbms.sql.method.DateGetDay2Method"/>
    <sql-method class="java.util.Date" method="getDay" datastore="postgresql"
                evaluator="org.datanucleus.store.rdbms.sql.method.DateGetDay3Method"/>
    <sql-method class="java.util.Date" method="getDate"
                evaluator="org.datanucleus.store.rdbms.sql.method.DateGetDayMethod"/>
    <sql-method class="java.util.Date" method="getDate" datastore="oracle"
                evaluator="org.datanucleus.store.rdbms.sql.method.DateGetDay2Method"/>
    <sql-method class="java.util.Date" method="getDate" datastore="postgresql"
                evaluator="org.datanucleus.store.rdbms.sql.method.DateGetDay3Method"/>
    <sql-method class="java.util.Date" method="getDate" datastore="sqlite"
                evaluator="org.datanucleus.store.rdbms.sql.method.DateGetDay4Method"/>
    <sql-method class="java.util.Date" method="getDate" datastore="firebird"
                evaluator="org.datanucleus.store.rdbms.sql.method.DateGetDay5Method"/>
    <sql-method class="java.util.Date" method="getMonth"
                evaluator="org.datanucleus.store.rdbms.sql.method.DateGetMonthMethod"/>
    <sql-method class="java.util.Date" method="getMonth" datastore="oracle"
                evaluator="org.datanucleus.store.rdbms.sql.method.DateGetMonth2Method"/>
    <sql-method class="java.util.Date" method="getMonth" datastore="postgresql"
                evaluator="org.datanucleus.store.rdbms.sql.method.DateGetMonth3Method"/>
    <sql-method class="java.util.Date" method="getMonth" datastore="sqlite"
                evaluator="org.datanucleus.store.rdbms.sql.method.DateGetMonth4Method"/>
    <sql-method class="java.util.Date" method="getMonth" datastore="firebird"
                evaluator="org.datanucleus.store.rdbms.sql.method.DateGetMonth5Method"/>
    <sql-method class="java.util.Date" method="getYear"
                evaluator="org.datanucleus.store.rdbms.sql.method.DateGetYearMethod"/>
    <sql-method class="java.util.Date" method="getYear" datastore="oracle"
                evaluator="org.datanucleus.store.rdbms.sql.method.DateGetYear2Method"/>
    <sql-method class="java.util.Date" method="getYear" datastore="postgresql"
                evaluator="org.datanucleus.store.rdbms.sql.method.DateGetYear3Method"/>
    <sql-method class="java.util.Date" method="getYear" datastore="sqlite"
                evaluator="org.datanucleus.store.rdbms.sql.method.DateGetYear4Method"/>
    <sql-method class="java.util.Date" method="getYear" datastore="firebird"
                evaluator="org.datanucleus.store.rdbms.sql.method.DateGetYear5Method"/>
    <sql-method class="java.util.Date" method="getHour"
                evaluator="org.datanucleus.store.rdbms.sql.method.DateGetHourMethod"/>
    <sql-method class="java.util.Date" method="getHour" datastore="oracle"
                evaluator="org.datanucleus.store.rdbms.sql.method.DateGetHour2Method"/>
    <sql-method class="java.util.Date" method="getHour" datastore="postgresql"
                evaluator="org.datanucleus.store.rdbms.sql.method.DateGetHour3Method"/>
    <sql-method class="java.util.Date" method="getHour" datastore="sqlserver"
                evaluator="org.datanucleus.store.rdbms.sql.method.DateGetHour4Method"/>
    <sql-method class="java.util.Date" method="getHour" datastore="sqlite"
                evaluator="org.datanucleus.store.rdbms.sql.method.DateGetHour5Method"/>
    <sql-method class="java.util.Date" method="getHour" datastore="firebird"
                evaluator="org.datanucleus.store.rdbms.sql.method.DateGetHour6Method"/>
    <sql-method class="java.util.Date" method="getMinute"
                evaluator="org.datanucleus.store.rdbms.sql.method.DateGetMinuteMethod"/>
    <sql-method class="java.util.Date" method="getMinute" datastore="oracle"
                evaluator="org.datanucleus.store.rdbms.sql.method.DateGetMinute2Method"/>
    <sql-method class="java.util.Date" method="getMinute" datastore="postgresql"
                evaluator="org.datanucleus.store.rdbms.sql.method.DateGetMinute3Method"/>
    <sql-method class="java.util.Date" method="getMinute" datastore="sqlserver"
                evaluator="org.datanucleus.store.rdbms.sql.method.DateGetMinute4Method"/>
    <sql-method class="java.util.Date" method="getMinute" datastore="sqlite"
                evaluator="org.datanucleus.store.rdbms.sql.method.DateGetMinute5Method"/>
    <sql-method class="java.util.Date" method="getMinute" datastore="firebird"
                evaluator="org.datanucleus.store.rdbms.sql.method.DateGetMinute6Method"/>
    <sql-method class="java.util.Date" method="getSecond"
                evaluator="org.datanucleus.store.rdbms.sql.method.DateGetSecondMethod"/>
    <sql-method class="java.util.Date" method="getSecond" datastore="oracle"
                evaluator="org.datanucleus.store.rdbms.sql.method.DateGetSecond2Method"/>
    <sql-method class="java.util.Date" method="getSecond" datastore="postgresql"
                evaluator="org.datanucleus.store.rdbms.sql.method.DateGetSecond3Method"/>
    <sql-method class="java.util.Date" method="getSecond" datastore="sqlserver"
                evaluator="org.datanucleus.store.rdbms.sql.method.DateGetSecond4Method"/>
    <sql-method class="java.util.Date" method="getSecond" datastore="hsql"
                evaluator="org.datanucleus.store.rdbms.sql.method.DateGetSecond5Method"/>
    <sql-method class="java.util.Date" method="getSecond" datastore="sqlite"
                evaluator="org.datanucleus.store.rdbms.sql.method.DateGetSecond6Method"/>
    <sql-method class="java.util.Date" method="getSecond" datastore="firebird"
                evaluator="org.datanucleus.store.rdbms.sql.method.DateGetSecond7Method"/>

    <sql-method class="java.util.Map" method="mapKey" evaluator="org.datanucleus.store.rdbms.sql.method.MapKeyMethod"/>
    <sql-method class="java.util.Map" method="mapValue" evaluator="org.datanucleus.store.rdbms.sql.method.MapValueMethod"/>
    <sql-method class="java.util.Map" method="containsEntry" evaluator="org.datanucleus.store.rdbms.sql.method.MapContainsEntryMethod"/>
    <sql-method class="java.util.Map" method="containsKey" evaluator="org.datanucleus.store.rdbms.sql.method.MapContainsKeyMethod"/>
    <sql-method class="java.util.Map" method="containsValue" evaluator="org.datanucleus.store.rdbms.sql.method.MapContainsValueMethod"/>
    <sql-method class="java.util.Map" method="get" evaluator="org.datanucleus.store.rdbms.sql.method.MapGetMethod"/>
    <sql-method class="java.util.Map" method="isEmpty" evaluator="org.datanucleus.store.rdbms.sql.method.MapIsEmptyMethod"/>
    <sql-method class="java.util.Map" method="size" evaluator="org.datanucleus.store.rdbms.sql.method.MapSizeMethod"/>

    <sql-method class="ARRAY" method="contains" evaluator="org.datanucleus.store.rdbms.sql.method.ArrayContainsMethod"/>
    <sql-method class="ARRAY" method="isEmpty" evaluator="org.datanucleus.store.rdbms.sql.method.ArrayIsEmptyMethod"/>
    <sql-method class="ARRAY" method="size" evaluator="org.datanucleus.store.rdbms.sql.method.ArraySizeMethod"/>
    <sql-method class="ARRAY" method="length" evaluator="org.datanucleus.store.rdbms.sql.method.ArraySizeMethod"/>
  </extension>

  <!-- SQL OPERATIONS -->
  <extension point="org.datanucleus.store.rdbms.sql_operation">
    <sql-operation name="mod" datastore="derby"
                   evaluator="org.datanucleus.store.rdbms.sql.operation.Mod2Operation"/>
    <sql-operation name="mod" datastore="h2"
                   evaluator="org.datanucleus.store.rdbms.sql.operation.Mod2Operation"/>
    <sql-operation name="mod" datastore="hsql"
                   evaluator="org.datanucleus.store.rdbms.sql.operation.Mod2Operation"/>
    <sql-operation name="mod" datastore="informix"
                   evaluator="org.datanucleus.store.rdbms.sql.operation.Mod2Operation"/>
    <sql-operation name="mod" datastore="oracle"
                   evaluator="org.datanucleus.store.rdbms.sql.operation.Mod2Operation"/>
    <sql-operation name="mod" datastore="db2"
                   evaluator="org.datanucleus.store.rdbms.sql.operation.Mod3Operation"/>

    <sql-operation name="concat" datastore="mysql"
                   evaluator="org.datanucleus.store.rdbms.sql.operation.Concat2Operation"/>
    <sql-operation name="concat" datastore="derby"
                   evaluator="org.datanucleus.store.rdbms.sql.operation.Concat3Operation"/>
    <sql-operation name="concat" datastore="db2"
                   evaluator="org.datanucleus.store.rdbms.sql.operation.Concat3Operation"/>

    <sql-operation name="numericToString"
                   evaluator="org.datanucleus.store.rdbms.sql.operation.NumericToStringOperation"/>
    <sql-operation name="numericToString" datastore="mysql"
                   evaluator="org.datanucleus.store.rdbms.sql.operation.NumericToString2Operation"/>
    <sql-operation name="numericToString" datastore="derby"
                   evaluator="org.datanucleus.store.rdbms.sql.operation.NumericToString3Operation"/>
  </extension>

  <!-- SQL TABLE NAMING -->
  <extension point="org.datanucleus.store.rdbms.sql_tablenamer">
    <sql-tablenamer name="t-scheme" class="org.datanucleus.store.rdbms.sql.SQLTableTNamer"/>
    <sql-tablenamer name="alpha-scheme" class="org.datanucleus.store.rdbms.sql.SQLTableAlphaNamer"/>
    <sql-tablenamer name="table-name" class="org.datanucleus.store.rdbms.sql.SQLTableNameNamer"/>
  </extension>

  <!-- DATASTORE ADAPTERS -->
  <extension point="org.datanucleus.store.rdbms.datastoreadapter">
    <datastore-adapter vendor-id="Adaptive Server Anywhere" class-name="org.datanucleus.store.rdbms.adapter.SybaseAdapter" priority="0"/>
    <datastore-adapter vendor-id="Adaptive Server Enterprise" class-name="org.datanucleus.store.rdbms.adapter.SybaseAdapter" priority="0"/>
    <datastore-adapter vendor-id="Adaptive Server IQ" class-name="org.datanucleus.store.rdbms.adapter.SybaseAdapter" priority="0"/>
    <datastore-adapter vendor-id="as/400" class-name="org.datanucleus.store.rdbms.adapter.DB2AS400Adapter" priority="1"/>
    <datastore-adapter vendor-id="cloudscape" class-name="org.datanucleus.store.rdbms.adapter.DerbyAdapter" priority="0"/>
    <datastore-adapter vendor-id="db2" class-name="org.datanucleus.store.rdbms.adapter.DB2Adapter" priority="0"/>
    <datastore-adapter vendor-id="derby" class-name="org.datanucleus.store.rdbms.adapter.DerbyAdapter" priority="0"/>
    <datastore-adapter vendor-id="firebird" class-name="org.datanucleus.store.rdbms.adapter.FirebirdAdapter" priority="0"/>
    <datastore-adapter vendor-id="h2" class-name="org.datanucleus.store.rdbms.adapter.H2Adapter" priority="0"/>
    <datastore-adapter vendor-id="hsql" class-name="org.datanucleus.store.rdbms.adapter.HSQLAdapter" priority="0"/>
    <datastore-adapter vendor-id="informix" class-name="org.datanucleus.store.rdbms.adapter.InformixAdapter" priority="0"/>
    <datastore-adapter vendor-id="interbase" class-name="org.datanucleus.store.rdbms.adapter.FirebirdAdapter" priority="0"/>
    <datastore-adapter vendor-id="mckoi" class-name="org.datanucleus.store.rdbms.adapter.McKoiAdapter" priority="0"/>
    <datastore-adapter vendor-id="microsoft" class-name="org.datanucleus.store.rdbms.adapter.MSSQLServerAdapter" priority="0"/>
    <datastore-adapter vendor-id="mysql" class-name="org.datanucleus.store.rdbms.adapter.MySQLAdapter" priority="0"/>
    <datastore-adapter vendor-id="nuodb" class-name="org.datanucleus.store.rdbms.adapter.NuoDBAdapter" priority="0"/>
    <datastore-adapter vendor-id="oracle" class-name="org.datanucleus.store.rdbms.adapter.OracleAdapter" priority="0"/>
    <datastore-adapter vendor-id="pointbase" class-name="org.datanucleus.store.rdbms.adapter.PointbaseAdapter" priority="0"/>
    <datastore-adapter vendor-id="postgresql" class-name="org.datanucleus.store.rdbms.adapter.PostgreSQLAdapter" priority="0"/>
    <datastore-adapter vendor-id="sap db" class-name="org.datanucleus.store.rdbms.adapter.SAPDBAdapter" priority="0"/>
    <datastore-adapter vendor-id="sapdb" class-name="org.datanucleus.store.rdbms.adapter.SAPDBAdapter" priority="0"/>
    <datastore-adapter vendor-id="sybase" class-name="org.datanucleus.store.rdbms.adapter.SybaseAdapter" priority="0"/>
    <datastore-adapter vendor-id="timesten" class-name="org.datanucleus.store.rdbms.adapter.TimesTenAdapter" priority="0"/>
    <datastore-adapter vendor-id="sqlite" class-name="org.datanucleus.store.rdbms.adapter.SQLiteAdapter" priority="0"/>
    <datastore-adapter vendor-id="virtuoso" class-name="org.datanucleus.store.rdbms.adapter.VirtuosoAdapter" priority="0"/>
  </extension>

  <!-- extension points from datanucleus-api-jdo-4.2.1 -->
  <!-- ANNOTATIONS -->
  <extension point="org.datanucleus.annotations">
    <annotations annotation-class="javax.jdo.annotations.PersistenceCapable" reader="org.datanucleus.api.jdo.metadata.JDOAnnotationReader"/>
    <annotations annotation-class="javax.jdo.annotations.PersistenceAware" reader="org.datanucleus.api.jdo.metadata.JDOAnnotationReader"/>
    <annotations annotation-class="javax.jdo.annotations.Queries" reader="org.datanucleus.api.jdo.metadata.JDOAnnotationReader"/>
    <annotations annotation-class="javax.jdo.annotations.Query" reader="org.datanucleus.api.jdo.metadata.JDOAnnotationReader"/>
  </extension>

  <!-- CALLBACK HANDLER -->
  <extension point="org.datanucleus.callbackhandler">
    <callback-handler class-name="org.datanucleus.api.jdo.JDOCallbackHandler" name="JDO"/>
  </extension>

  <!-- METADATA MANAGER -->
  <extension point="org.datanucleus.metadata_manager">
    <metadata-manager class="org.datanucleus.api.jdo.metadata.JDOMetaDataManager" name="JDO"/>
  </extension>

  <!-- PERSISTENCE API -->
  <extension point="org.datanucleus.api_adapter">
    <api-adapter name="JDO" class-name="org.datanucleus.api.jdo.JDOAdapter"/>
  </extension>
</plugin>
