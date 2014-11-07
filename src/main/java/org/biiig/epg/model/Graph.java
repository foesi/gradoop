package org.biiig.epg.model;

/**
 * Created by martin on 05.11.14.
 */
public interface Graph extends Identifiable, Attributed, Labeled {
  Iterable<Long> getVertices();
}
