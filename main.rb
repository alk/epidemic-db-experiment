#!/usr/bin/env ruby

require 'logger'

class VClock
  attr_reader :list
  def initialize(list = [])
    @list = list.sort
  end
  def ==(other_clock)
    other_clock.kind_of?(VClock) && other_clock.list == @list
  end
  def hash
    @list.hash
  end
  def dup
    list = @list
    @list = list.dup
    rv = super
    @list = list
    rv
  end
  def bump!(node_id, times = 1)
    raise unless times > 0 && times.kind_of?(Integer)
    idx = @list.index {|el| el[0] == node_id}
    if idx
      pair = @list[idx]
      pair[1] += times
    else
      @list << [node_id, times]
      @list.sort_by! {|p| p[0]}
    end
    self
  end

  def descends_from?(other_clock)
    mylist = @list
    otherlist = other_clock.list
    myidx = 0
    otherlist.each_with_index do |pair, idx|
      node = pair[0]
      while true
        mypair = mylist[myidx]
        return false unless mypair

        break if mypair[0] == node

        if mypair[0] < node
          myidx += 1
        else
          return false
        end
      end

      raise unless pair[0] == mypair[0]
      return false if mypair[1] < pair[1]

      myidx += 1
    end
    true
  end

  def each_plists_merging a, b
    asize = a.size
    bsize = b.size
    ia = 0
    ib = 0
    while ia < asize && ib < bsize
      node_a, c_a = *a[ia]
      node_b, c_b = *b[ib]
      if node_a == node_b
        yield node_a, c_a, c_b, ia, ib
        ia += 1
        ib += 1
        next
      end
      if node_a < node_b
        yield node_a, c_a, nil, ia, nil
        ia += 1
      else
        yield node_b, nil, c_b, nil, ib
        ib += 1
      end
    end
    if ia < asize
      while ia < asize
        node_a, c_a = *a[ia]
        yield node_a, c_a, nil, ia, nil
        ia += 1
      end
    elsif ib < bsize
      while ib < bsize
        node_b, c_b = *b[ib]
        yield node_b, nil, c_b, nil, ib
        ib += 1
      end
    end
  end

  def merge_with!(other_clock)
    new_list = []
    each_plists_merging @list, other_clock.list do |node, c_l, c_r|
      new_list << [node,
                   if c_l && c_r && c_r > c_l
                     c_r
                   elsif c_l
                     c_l
                   else
                     c_r
                   end]
    end
    @list = new_list
    self
  end

  def total_changes
    @list.inject(0) do |s, (_, c)|
      s + c
    end
  end

  def to_s
    "#<VClock " << @list.map {|(node, count)| "#{node}: #{count}"}.join(", ") << ">"
  end
end

class Database
  TOMBSTONE = Object.new

  @@logger = Logger.new(STDERR)
  @@logger.level = Logger::DEBUG

  attr_reader :node_id

  def initialize(node_id, initial_triples = [])
    @node_id = node_id
    @values = {}
    @clocks = {}
    initial_triples.each do |k,v,c|
      @values[k] = v
      @clocks[k] = c
    end
  end

  def put(k, v)
    return if v == @values[k]
    clock = @clocks[k]
    if clock
      @clocks[k] = clock.dup.bump!(@node_id)
    else
      @clocks[k] = VClock.new.bump!(@node_id)
    end
    @values[k] = v
  end

  def delete(k)
    raise "no_value" unless get(k)
    put(k, TOMBSTONE)
  end

  def get_full(k)
    c = @clocks[k]
    return unless c
    [@values[k], c]
  end

  def get_triple(k)
    c = @clocks[k]
    raise unless c
    [k, @values[k], c]
  end

  def get(k)
    v = @values[k]
    (v == TOMBSTONE) ? nil : v
  end

  def each_triple
    @values.each_pair do |k, v|
      yield k, v, @clocks[k]
    end
  end

  def compute_signature_inner(size = nil, fuzz = nil)
    size ||= Math.sqrt(@values.size).to_i
    size = 1 if size == 0
    fuzz ||= rand(0x1FFFFFFF)
    rv = Array.new(size+1)
    rv[size] = []
    @clocks.each_pair do |k, c|
      bin = k.hash % size
      (rv[bin] ||= []) << [k, c, @values[k], fuzz]
    end
    sig = rv.map {|stuff| (stuff || []).map {|c| c.hash}.sort.hash }
    sig[size] = fuzz
    [sig, rv]
  end

  def compute_signature(size = nil, fuzz = nil)
    compute_signature_inner(size, fuzz).first
  end

  def collect_diffing_triples_and_sig(other_signature)
    sig, stuffs = *compute_signature_inner(other_signature.size - 1,
                                           other_signature[-1])
    rv = []
    sig.each_with_index do |s, i|
      next if other_signature[i] == s
      stuffs[i].each do |(k,_)|
        rv << [k, @values[k], @clocks[k]]
      end
    end
    [rv, sig]
  end

  def log_conflict(k, local_c, remote_c, picked)
    @@logger.warn "replication conflict on key #{k.inspect}. (local: #{local_c}, remote: #{remote_c}). Picked #{picked.inspect}"
  end

  def add(k, v, clock)
    my_c = @clocks[k]
    if !my_c || clock.descends_from?(my_c)
      return false if my_c == clock
      @values[k] = v
      @clocks[k] = clock
      return true
    end

    if my_c.descends_from?(clock)
      return false
    end

    if v == @values[k]
      log_conflict k, my_c, clock, :same_value
      my_c = my_c.dup
      @clocks[k] = my_c.merge_with! clock
      return true
    end


    case my_c.total_changes <=> clock.total_changes
    when 0
      log_conflict k, my_c, clock, :same_changes_count_took_local
      my_c = my_c.dup
      my_c.merge_with!(clock).bump!(@node_id)
    when 1
      log_conflict k, my_c, clock, :local
      my_c = my_c.dup
      my_c.merge_with! clock
    when -1
      log_conflict k, my_c, clock, :remote
      my_c = my_c.dup
      my_c.merge_with! clock
      @values[k] = v
    end

    @clocks[k] = my_c

    return true
  end

  def add_changes(triples)
    changed_keys = []
    triples.each do |(k, v, c)|
      changed = self.add k, v, c
      changed_keys << k if changed
    end
    changed_keys
  end

  def pull_from(other_db)
    local_sig = self.compute_signature
    remote_tuples, remote_sig = *other_db.collect_diffing_triples_and_sig(local_sig)
    rv = self.add_changes remote_tuples
    if block_given?
      yield rv, remote_sig
    end
    rv
  end

  def pull_push_with(other_db)
    self.pull_from other_db do |_, remote_sig|
      push_tuples, = *self.collect_diffing_triples_and_sig(remote_sig)
      other_db.add_changes push_tuples
    end
  end
end
