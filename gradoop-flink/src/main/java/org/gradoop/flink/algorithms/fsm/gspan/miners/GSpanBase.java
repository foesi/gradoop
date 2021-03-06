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


package org.gradoop.flink.algorithms.fsm.gspan.miners;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.flink.algorithms.fsm.gspan.api.GSpanMiner;

/**
 * Abstract superclass of Flink GSpan implementations.
 */
public abstract class GSpanBase implements GSpanMiner {

  /**
   * Flink execution environment
   */
  private ExecutionEnvironment executionEnvironment;

  @Override
  public void setExecutionEnvironment(ExecutionEnvironment env) {
    this.executionEnvironment = env;
  }

  /**
   * Getter.
   *
   * @return Flink execution environment
   */
  protected ExecutionEnvironment getExecutionEnvironment() {
    return executionEnvironment;
  }

}
