package zrz.triplerocks.loader;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;

import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.system.ErrorHandlerFactory;

import com.google.common.base.Stopwatch;

import zrz.rocksdb.JRocksEngine;

/**
 * 
 * @author theo
 *
 */

public class TripleRocksLoader {

  public static void main(String[] args) {

    try (JRocksEngine db = JRocksEngine.open(Paths.get("/tmp/import555"))) {

      JenaBatchedSink sink = new JenaBatchedSink(db);

      Stopwatch start = Stopwatch.createStarted();
      try (InputStream in = new FileInputStream("/Users/theo/Downloads/lexvo_latest.rdf")) {
        RDFParser.create()
            .source(in)
            .lang(RDFLanguages.RDFXML)
            .errorHandler(ErrorHandlerFactory.errorHandlerWarn)
            .parse(sink);
      }
      catch (FileNotFoundException e) {
        // TODO Auto-generated catch block
        throw new RuntimeException(e);
      }
      catch (IOException e) {
        // TODO Auto-generated catch block
        throw new RuntimeException(e);
      }

      System.err.println("Procssing: " + start.stop());

      Stopwatch flushing = Stopwatch.createStarted();

      db.compactRange();

      db.flush();

      System.err.println("Flushing: " + flushing.stop());

      System.err.println("TOTAL: " + flushing.elapsed().plus(start.elapsed()));

    }

  }

}
