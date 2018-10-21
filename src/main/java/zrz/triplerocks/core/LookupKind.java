package zrz.triplerocks.core;

/**
 * in many cases, the actual values of the triples isn't used, rather than following relationships in the store. We can
 * save a lot of space by using internal keys for each IRI/BN rather than the qualified name. obviously such a mechanism
 * may not be beneficial for applications which are heavily data value oriented.
 * 
 * when such a reference key is used, we can defer pulling out the actual value until it is needed. and an java layer
 * cache can avoid us even hitting that in a lot of use cases. we can use mget() to avoid some JNI overhead too when
 * retrieving the key values.
 * 
 * keys which are no longer used will eventually need to be removed rather than pile up, so we have a couple of options.
 * the first is to keep a reference count of keys - although this means we can't have a writer adding a key in parallel
 * to removing one. so either a single write processor, or keeping track of which predicates are used.
 * 
 * the second option is periodic background cleanup. in some scenarios (such as where we perform intensive stats
 * collection for queries) this might not be such as bad option.
 * 
 * @author theo
 *
 */
public enum LookupKind {

  /**
   * index for looking up the internal ID of a key.
   */

  KEY_TO_ID,

  /**
   * maps each IRI ID to its literal value.
   */

  ID_TO_KEY,

  ;

}
