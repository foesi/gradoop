package org.biiig.epg.model;

/**
 * Created by martin on 05.11.14.
 */
public interface Attributed {
  Iterable<String> getPropertyKeys();

  Object getProperty(String key);
}
