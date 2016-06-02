/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.impl.algorithms.fsm.common.tuples;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.flink.api.java.tuple.Tuple1;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.DfsCode;

import java.io.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * tuple-representation fo a compressed DFS code including its support.
 * (bytes,support)
 */
public class CompressedDfsCode extends Tuple1<byte[]> {
  
  /**
   * default constructor
   */
  public CompressedDfsCode() {
  }

  /**
   * valued constructor
   * @param dfsCode DFS code to compress
   */
  public CompressedDfsCode(DfsCode dfsCode) {
    try {
      ByteArrayOutputStream byteArrayOS = new ByteArrayOutputStream();
      GZIPOutputStream gzipOS = new GZIPOutputStream(byteArrayOS);
      ObjectOutputStream objectOS = new ObjectOutputStream(gzipOS);
      objectOS.writeObject(dfsCode);
      objectOS.close();
      this.f0 = byteArrayOS.toByteArray();
    } catch (IOException e) {
      this.f0 = new byte[0];
    }
  }

  /**
   * uncompressing the store DFS code
   * @return uncompressed DFS code
   */
  public DfsCode getDfsCode() {
    DfsCode dfsCode;

    try {
      ByteArrayInputStream byteArrayIS = new ByteArrayInputStream(this.f0);
      GZIPInputStream gzipIn = new GZIPInputStream(byteArrayIS);
      ObjectInputStream objectIn = new ObjectInputStream(gzipIn);
      dfsCode = (DfsCode) objectIn.readObject();
      objectIn.close();
    } catch (IOException | ClassNotFoundException e) {
      dfsCode = new DfsCode();
    }

    return dfsCode;
  }

  @Override
  public String toString() {
    return getDfsCode().toString();
  }

  @Override
  public int hashCode() {

    HashCodeBuilder builder = new HashCodeBuilder();

    for (byte b : this.f0) {
      builder.append(b);
    }

    return builder.hashCode();

  }

  @Override
  public boolean equals(Object o) {
    boolean equals = o != null && o.getClass() == this.getClass();

    if (equals) {
      byte[] ownBytes = this.getBytes();
      byte[] otherBytes = ((CompressedDfsCode) o).getBytes();

      equals = ownBytes.length == otherBytes.length;

      if (equals) {

        for (int i = 0; i < this.getBytes().length; i++) {
          equals = ownBytes[i] == otherBytes[i];
          if (! equals) {
            break;
          }
        }
      }
    }
    return equals;
  }

  private byte[] getBytes() {
    return this.f0;
  }

}
