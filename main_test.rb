require 'test/unit'

require './main'

class VClockTest < Test::Unit::TestCase
  def test_just
    clock_a = VClock.new
    clock_b = VClock.new
    assert(clock_a.descends_from?(clock_b))
    assert(clock_b.descends_from?(clock_a))

    clock_a.bump!(:node1)
    assert(clock_a.descends_from?(clock_b))
    assert(!clock_b.descends_from?(clock_a))

    clock_b.bump!(:node2)
    assert(!clock_a.descends_from?(clock_b))
    assert(!clock_b.descends_from?(clock_a))

    clock_b.bump!(:node1)
    assert(clock_b.descends_from?(clock_a))

    clock_b.bump!(:node2)
    assert(clock_b.descends_from?(clock_a))

    clock_a.bump!(:node1)
    assert(!clock_b.descends_from?(clock_a))
    assert(!clock_a.descends_from?(clock_b))

    clock_b.bump!(:node1)
    assert(clock_b.descends_from?(clock_a))

    clock_b = clock_a.dup
    assert(clock_a.descends_from?(clock_b))
    assert(clock_b.descends_from?(clock_a))

    clock_b.bump!(:node2)
    assert(clock_b.descends_from?(clock_a))
  end

  def test_eql_and_dup
    clock_a = VClock.new
    clock_b = clock_a.dup
    assert_equal clock_a, clock_b
    assert(!clock_a.equal?(clock_b))
    assert(!clock_a.list.equal?(clock_b.list))

    clock_a.bump!(:node_c)
    assert_not_equal clock_a, clock_b

    clock_b.bump!(:node_c)
    assert_equal clock_a, clock_b
  end

  def test_to_s
    clock = VClock.new
    s = clock.bump!(:node1).bump!(:node1).bump!(:node2).bump!(:node1).to_s
    assert_equal "#<VClock node1: 3, node2: 1>", s
  end

  def plist_mergings_enum(a, b)
    Enumerator.new do |y|
      VClock.new.each_plists_merging a, b do |node, c_a, c_b|
        y << [node, c_a, c_b]
      end
    end
  end

  def test_each_plists_merging
    a = [[:a, 1], [:b, 2]]
    b = [[:ac, 4], [:b, 3], [:c, 5]]

    val1 = plist_mergings_enum(a, b).to_a
    assert_equal [[:a, 1, nil], [:ac, nil, 4], [:b, 2, 3], [:c, nil, 5]], val1

    val2 = plist_mergings_enum(b, a).to_a
    assert_equal [[:a, nil, 1], [:ac, 4, nil], [:b, 3, 2], [:c, 5, nil]], val2

    val3 = plist_mergings_enum([], a).to_a
    assert_equal [[:a, nil, 1], [:b, nil, 2]], val3

    val4 = plist_mergings_enum(a, []).to_a
    assert_equal [[:a, 1, nil], [:b, 2, nil]], val4
  end

  def test_merge_with
    a = VClock.new.bump!(:b).bump!(:a).bump!(:a).bump!(:c)
    assert_equal VClock.new.bump!(:a).bump!(:c).bump!(:b).bump!(:a), a
    b = VClock.new.bump!(:b).bump!(:b).bump!(:a)
    assert !a.descends_from?(b)
    assert !b.descends_from?(a)
    merged = a.merge_with!(b)
    assert merged.equal?(a)
    assert a.descends_from?(b)
    assert !b.descends_from?(a)
    assert_equal [[:a, 2], [:b, 2], [:c, 1]], a.list
  end
end


class DBTest < Test::Unit::TestCase
  def test_empty_replication
    db_a = Database.new(:a)
    db_b = Database.new(:b)

    db_a.pull_from db_b
    db_b.pull_from db_a
    db_a.pull_from db_a
    assert_equal [], db_a.to_enum(:each_triple).to_a
    assert_equal [], db_b.to_enum(:each_triple).to_a
  end

  def extract_pairs db
    db.to_enum(:each_triple).map {|k, v, _| [k, v]}.sort
  end

  def test_simple_ops
    db = Database.new(:a)

    assert_equal nil, db.get(:k)

    db.put :k, 1
    assert_equal 1, db.get(:k)

    assert_equal [1, VClock.new.bump!(:a)], db.get_full(:k)

    db.put :k, 3

    assert_equal [:k, 3, VClock.new.bump!(:a, 2)], db.get_triple(:k)

    db.delete :k

    assert_equal [:k, Database::TOMBSTONE, VClock.new.bump!(:a, 3)], db.get_triple(:k)

    assert_equal nil, db.get(:k)

    assert_raises RuntimeError do
      db.delete :k
    end

    db.put :a, :b

    assert_equal([[:a, :b, VClock.new.bump!(:a)],
                  [:k, Database::TOMBSTONE, VClock.new.bump!(:a, 3)]],
                 Enumerator.new {|y| db.each_triple {|k, v, c| y << [k, v, c]}}.to_a.sort)
  end

  def test_trivial_replication
    db_a = Database.new(:a)
    db_a.put :k, :v
    db_b = Database.new(:b)
    db_b.pull_from db_a
    assert_equal [[:k, :v]], extract_pairs(db_b)
    assert_equal [[:k, :v]], extract_pairs(db_a)
  end

  def test_simple_replication
    db_a = Database.new(:a)
    db_b = Database.new(:b)

    db_a.put :k1, 1
    db_b.put :k2, 2

    assert_equal [[:k1, 1]], extract_pairs(db_a)
    assert_equal [[:k2, 2]], extract_pairs(db_b)

    changed_keys = db_b.pull_from db_a

    assert_equal [:k1], changed_keys

    assert_equal [[:k1, 1]], extract_pairs(db_a)
    assert_equal [[:k1, 1], [:k2, 2]], extract_pairs(db_b)

    k1_triple = db_a.get_triple :k1
    changed_keys = db_a.pull_from db_b
    assert_equal [:k2], changed_keys

    assert_equal k1_triple, db_a.get_triple(:k1)

    assert_equal [[:k1, 1], [:k2, 2]], extract_pairs(db_a)
    assert_equal [[:k1, 1], [:k2, 2]], extract_pairs(db_b)

    db_b.put :k1, 3
    assert_equal 3, db_b.get(:k1)

    changed_keys = db_a.pull_from db_b
    assert_equal [:k1], changed_keys

    assert_equal 3, db_a.get(:k1)

    changed_keys = db_b.pull_from db_a
    assert_equal [], changed_keys
  end

  def test_pull_push
    db_a = Database.new(:a)
    db_b = Database.new(:b)
    [[:a, 1], [:b, 2], [:c, 3]].each {|(k,v)| db_a.put k, v}
    [[:z, :z], [:y, :y], [:x, :x]].each {|(k, v)| db_b.put k, v}

    changed_on_a_size = db_a.pull_push_with db_b
    assert_equal [:x, :y, :z], changed_on_a_size.sort

    assert_equal extract_pairs(db_a), extract_pairs(db_b)
    assert_equal db_a.instance_variable_get(:@clocks), db_b.instance_variable_get(:@clocks)
  end

  def test_signatures_work
    db_a = Database.new(:a)
    db_b = Database.new(:b)

    initial_pairs = (0...100).map do |k|
      [k, [k, :a].hash]
    end

    initial_pairs.each do |(k, v)|
      db_a.put k, v
    end

    db_b.pull_from db_a

    assert_equal initial_pairs.sort, extract_pairs(db_b).sort

    assert_equal [], db_b.pull_from(db_a)

    diffing_b, _ = db_b.collect_diffing_triples_and_sig(db_a.compute_signature)
    assert_equal [], diffing_b

    db_a.put :a, :b

    diffing_a, _ = db_a.collect_diffing_triples_and_sig(db_b.compute_signature(nil, 0))
    assert diffing_a.map(&:first).include?(:a)
    assert diffing_a.size < initial_pairs.size / 3
  end

  module LogConflictInterceptor
    attr_reader :conflict_calls
    def log_conflict_with_interceptor(*args)
      (@conflict_calls ||= []) << args
    end
    def self.included(klass)
      klass.send :alias_method, :log_conflict_without_interceptor, :log_conflict
      klass.send :alias_method, :log_conflict, :log_conflict_with_interceptor
    end

    def self.extended_db *args
      db = Database.new(*args)
      class << db
        include LogConflictInterceptor
      end
      db
    end
  end

  def test_log_conflict_interceptor
    db = LogConflictInterceptor.extended_db(:a)
    db.log_conflict_with_interceptor :call_a
    assert_equal [[:call_a]], db.conflict_calls
    db.log_conflict :call_b, 1, 2, 3
    assert_equal [[:call_a], [:call_b, 1, 2, 3]], db.conflict_calls
  end

  def simple_vclock(*args)
    VClock.new(args.each_slice(2).to_a)
  end

  def test_conflicts
    db = LogConflictInterceptor.extended_db(:a)
    db.put :k, :v
    db.put :k, :v2
    assert_equal [:v2, simple_vclock(:a, 2)], db.get_full(:k)
    db.add :k, :v2, simple_vclock(:b, 1)
    assert_equal [[:k, VClock.new.bump!(:a, 2), VClock.new.bump!(:b, 1), :same_value]], db.conflict_calls
    assert_equal [:v2, simple_vclock(:a, 2, :b, 1)], db.get_full(:k)

    db.conflict_calls.clear
    db.add :k, :v2, simple_vclock(:b, 1)
    assert_equal [], db.conflict_calls

    db.add :k, :v3, simple_vclock(:b, 2)
    # local has more mutations
    assert_equal [[:k, simple_vclock(:a, 2, :b, 1), simple_vclock(:b, 2), :local]], db.conflict_calls
    assert_equal [:v2, simple_vclock(:a, 2, :b, 2)], db.get_full(:k)

    db.conflict_calls.clear
    db.add :k, :v3, simple_vclock(:b, 2)
    assert_equal [], db.conflict_calls

    db.add :k, :v4, simple_vclock(:a, 1, :b, 3)
    assert_equal [[:k, simple_vclock(:a, 2, :b, 2), simple_vclock(:a, 1, :b, 3), :same_changes_count_took_local]], db.conflict_calls
    # note: bumped a's counter
    assert_equal [:v2, simple_vclock(:a, 3, :b, 3)], db.get_full(:k)

    db.conflict_calls.clear
    db.add :k, :v4, simple_vclock(:a, 1, :b, 3)
    assert_equal [], db.conflict_calls

    db.add :k, :v5, simple_vclock(:a, 2, :b, 6)
    # remote has more mutations
    assert_equal [[:k, simple_vclock(:a, 3, :b, 3), simple_vclock(:a, 2, :b, 6), :remote]], db.conflict_calls
    assert_equal [:v5, simple_vclock(:a, 3, :b, 6)], db.get_full(:k)

    db.conflict_calls.clear
    db.add :k, :v5, simple_vclock(:a, 2, :b, 6)
    assert_equal [], db.conflict_calls
  end

end
