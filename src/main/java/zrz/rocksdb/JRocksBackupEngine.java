package zrz.rocksdb;

import java.nio.file.Path;

import org.rocksdb.BackupEngine;
import org.rocksdb.BackupableDBOptions;
import org.rocksdb.Checkpoint;
import org.rocksdb.Env;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import com.google.common.io.Files;

import zrz.triplerocks.core.BaseRocksTripleStore;

public class JRocksBackupEngine {

  private BackupEngine engine;

  public JRocksBackupEngine(Path path) {
    BackupableDBOptions opts = new BackupableDBOptions(path.toString());
    try {
      this.engine = BackupEngine.open(Env.getDefault(), opts);
    }
    catch (RocksDBException e) {
      // TODO Auto-generated catch block
      throw new RuntimeException(e);
    }
  }

  public JRocksBackup backup(RocksDB db) {

    try {
      engine.createNewBackupWithMetadata(db, "xxx", true);
    }
    catch (RocksDBException e) {
      // TODO Auto-generated catch block
      throw new RuntimeException(e);
    }

    engine.getBackupInfo()
        .forEach(info -> {
          System.err.println(info.appMetadata());
          System.err.println(info.backupId());
          System.err.println(info.timestamp());
          System.err.println(info.size());
          System.err.println(info.numberFiles());
        });

    return new JRocksBackup();

  }

  public void close() {
    this.engine.close();
  }

  public JRocksBackup backup(BaseRocksTripleStore store) {
    return backup(store.db());
  }

}
