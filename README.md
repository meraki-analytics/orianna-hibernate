# Orianna Hibernate

Hibernate support for [Orianna](https://github.com/robrua/Orianna/).
 
## Setup

Just [download](https://github.com/robrua/orianna-hibernate/releases) the latest .jar and add it to your project's build path. You'll need to import the base Orianna resources as well.
 
To do this in eclipse, I recommend creating a lib/ directory in your project's root directory and putting the .jar there. Then just right click the .jar in eclipse and click Build Path -> Add to Build Path.

You can also find the latest SNAPSHOT build [here](http://robrua.com/orianna).

If you use Maven to manage your dependencies, Orianna Hibernate is posted on Maven Central:

```xml
<dependency>
  <groupId>com.robrua</groupId>
  <artifactId>orianna-hibernate</artifactId>
  <version>2.2.5</version>
</dependency>
```

Or using Gradle:

```
repositories {
    mavenCentral()
}

dependencies {
	compile 'com.robrua:orianna-hibernate:2.2.5'
}
```

## Dependencies

Orianna Hibernate relies on [Hibernate](http://hibernate.org/) v4.3.8.Final. It is included in the JARs distributed here.
 
## Usage

The HibernateDB is a DataStore as defined by Orianna. It is instantiated using either the builder pattern or a Hibernate configuration object.

The builder will use MySQL settings by default, though you'll need to include the [connector](http://mvnrepository.com/artifact/mysql/mysql-connector-java) yourself. Any other hibernate-supported database can be used instead.

Make sure you close your DB when you're finished, as well as closing any iterators you may use.

```java
package com.robrua.orianna.test.core;

import com.robrua.orianna.api.core.RiotAPI;
import com.robrua.orianna.store.HibernateDB;
import com.robrua.orianna.type.core.common.Region;

public class Example {
    public static void main(String[] args) {
        RiotAPI.setMirror(Region.NA);
        RiotAPI.setRegion(Region.NA);
        RiotAPI.setAPIKey("YOUR-API-KEY-HERE");
        
        HibernateDB db = HibernateDB.builder().URL("jdbc:mysql://localhost/orianna").username("MYSQLUSER").password("MYSQLPASSWORD").build();
        RiotAPI.setDataStore(db);
        
        // Do your thing here. Data will be cached in the Hibernate DB as it would with the standard in-memory cache.
        
        db.close();
    }
}
```

## Download
[Releases](https://github.com/robrua/orianna-hibernate/releases)/[Snapshot](http://robrua.com/orianna)

## Questions/Contributions
Feel free to send pull requests or to contact me via github or email (robrua@alumni.cmu.edu).

## Disclaimer
Orianna Hibernate isn't endorsed by Riot Games and doesn't reflect the views or opinions of Riot Games or anyone officially involved in producing or managing League of Legends. League of Legends and Riot Games are trademarks or registered trademarks of Riot Games, Inc. League of Legends © Riot Games, Inc.
