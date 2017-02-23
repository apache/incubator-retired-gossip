/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gossip.crdt;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import org.apache.gossip.crdt.OrSet.Builder.Operation;

/*
 * A immutable set 
 */
public class OrSet<E>  implements Crdt<Set<E>, OrSet<E>> {
  
  private final Map<E, Set<UUID>> elements = new HashMap<>();
  private final Map<E, Set<UUID>> tombstones = new HashMap<>();
  private final transient Set<E> val;
  
  public OrSet(){
    val = computeValue();
  }
  
  OrSet(Map<E, Set<UUID>> elements, Map<E, Set<UUID>> tombstones){
    this.elements.putAll(elements);
    this.tombstones.putAll(tombstones);
    val = computeValue();
  }
  
  @SafeVarargs
  public OrSet(E ... elements){
    for (E e: elements){
      internalAdd(e);
    }
    val = computeValue();
  }
  
  public OrSet(Builder<E>builder){
    for (Builder<E>.OrSetElement<E> e: builder.elements){
      if (e.operation == Operation.ADD){
        internalAdd(e.element);
      } else {
        internalRemove(e.element);
      }
    }
    val = computeValue();
  }
  
  /**
   * This constructor is the way to remove elements from an existing set
   * @param set
   * @param builder 
   */
  public OrSet(OrSet<E> set, Builder<E> builder){
    elements.putAll(set.elements);
    tombstones.putAll(set.tombstones);
    for (Builder<E>.OrSetElement<E> e: builder.elements){
      if (e.operation == Operation.ADD){
        internalAdd(e.element);
      } else {
        internalRemove(e.element);
      }
    }
    val = computeValue();
  }

  public OrSet(OrSet<E> left, OrSet<E> right){
    elements.putAll(left.elements);
    elements.putAll(right.elements);
    tombstones.putAll(left.tombstones);
    tombstones.putAll(right.tombstones);
    val = computeValue();
  }
  
  public OrSet.Builder<E> builder(){
    return new OrSet.Builder<>();
  }
  
  @Override
  public OrSet<E> merge(OrSet<E> other) {
    return new OrSet<E>(this, other);
  }
  
  private void internalAdd(E element){
    Set<UUID> l = elements.get(element);
    if (l == null){
      Set<UUID> d = new HashSet<UUID>();
      d.add(UUID.randomUUID());
      elements.put(element, d);
    } else {
      l.add(UUID.randomUUID());
    }
  }
  
  private void internalRemove(E element){
    Set<UUID> elementIds = elements.get(element);
    if (elementIds == null){
      //deleting elements not in the list
      return;
    }
    Set<UUID> current = tombstones.get(element);
    if (current != null){
      current.addAll(elementIds);
    } else {
      tombstones.put(element, elementIds);
    }
  }

  /*
   * Computes the live values by analyzing the elements and tombstones
   */
  private Set<E> computeValue(){
    Set<E> values = new HashSet<>();
    for (Entry<E, Set<UUID>> entry: elements.entrySet()){
      if (entry.getValue() == null || entry.getValue().size() == 0){
        continue;
      }
      Set<UUID> deleteIds = tombstones.get(entry.getKey());
      if (deleteIds == null){
        values.add(entry.getKey());
      } else {
        if (!deleteIds.containsAll(entry.getValue())){
          values.add(entry.getKey());
        } else {
          //if all the entry uuid is deleted the entry is deleted
        }
      }
    }
    return values;
  }
  
  @Override
  public Set<E> value() {
    return val;
  }

  @Override
  public OrSet<E> optimize() {
    return this;
  }
  
  public static class Builder<E> {
    public static enum Operation {
      ADD, REMOVE
    };

    private class OrSetElement<EL> {
      EL element;
      Operation operation;

      private OrSetElement(EL element, Operation operation) {
        this.element = element;
        this.operation = operation;
      }
    }

    private List<OrSetElement<E>> elements = new ArrayList<>();

    public Builder<E> add(E element) {
      elements.add(new OrSetElement<E>(element, Operation.ADD));
      return this;
    }

    public Builder<E> remove(E element) {
      elements.add(new OrSetElement<E>(element, Operation.REMOVE));
      return this;
    }

    public Builder<E> mutate(E element, Operation operation) {
      elements.add(new OrSetElement<E>(element, operation));
      return this;
    }
  }

  
  public int size() {
    return value().size();
  }

  
  public boolean isEmpty() {
    return value().size() == 0;
  }

  
  public boolean contains(Object o) {
    return value().contains(o);
  }

  
  public Iterator<E> iterator() {
    Iterator<E> managed = value().iterator();
    return new Iterator<E>() {

      @Override
      public void remove() {
        throw new IllegalArgumentException();
      }

      @Override
      public boolean hasNext() {
        return managed.hasNext();
      }

      @Override
      public E next() {
        return managed.next();
      }
      
    };
  }

  public Object[] toArray() {
    return value().toArray();
  }

  public <T> T[] toArray(T[] a) {
    return value().toArray(a);
  }

  public boolean add(E e) {
    throw new IllegalArgumentException("Can not add");
  }


  public boolean remove(Object o) {
    throw new IllegalArgumentException();
  }

  public boolean containsAll(Collection<?> c) {
    return this.value().containsAll(c);
  }

  public boolean addAll(Collection<? extends E> c) {
    throw new IllegalArgumentException();
  }

  public boolean retainAll(Collection<?> c) {
    throw new IllegalArgumentException();
  }

  public boolean removeAll(Collection<?> c) {
    throw new IllegalArgumentException();
  }

  public void clear() {
    throw new IllegalArgumentException();
  }

  @Override
  public String toString() {
    return "OrSet [elements=" + elements + ", tombstones=" + tombstones + "]" ;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((value() == null) ? 0 : value().hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    @SuppressWarnings("rawtypes")
    OrSet other = (OrSet) obj;
    if (elements == null) {
      if (other.elements != null)
        return false;
    } else if (!value().equals(other.value()))
      return false;
    return true;
  }

  Map<E, Set<UUID>> getElements() {
    return elements;
  }

  Map<E, Set<UUID>> getTombstones() {
    return tombstones;
  }

}
