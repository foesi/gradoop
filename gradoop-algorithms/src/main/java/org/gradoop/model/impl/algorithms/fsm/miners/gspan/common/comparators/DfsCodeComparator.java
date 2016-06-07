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

package org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.comparators;



import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos.DfsCode;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos.DfsStep;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Iterator;

/**
 * Step-wise comparator for DFS codes.
 */
public class DfsCodeComparator implements Comparator<DfsCode>, Serializable {

  /**
   * step comparator
   */
  private final DfsStepComparator dfsStepComparator;

  /**
   * constructor
   * @param directed true for comparing DFS codes of directed graphs
   */
  public DfsCodeComparator(boolean directed) {
    dfsStepComparator = new DfsStepComparator(directed);
  }

  @Override
  public int compare(DfsCode c1, DfsCode c2) {
    int comparison = 0;

    Iterator<DfsStep> i1 = c1.getSteps().iterator();
    Iterator<DfsStep> i2 = c2.getSteps().iterator();

    // compare steps until there is a difference
    while (comparison == 0 && i1.hasNext() && i2.hasNext()) {
      DfsStep s1 = i1.next();
      DfsStep s2 = i2.next();

      comparison = dfsStepComparator.compare(s1, s2);
    }

    // common parent
    if (comparison == 0) {
      // c1 is child of c2
      if (i1.hasNext()) {
        comparison = 1;
        // c2 is child of c1
      } else if (i2.hasNext()) {
        comparison = -1;
      }
    }

    return comparison;
  }
}
