package zrz.triplerocks.api;

public interface TR_TripleFactory<TripleT> {

  TR_NodeFactory<TripleT> slot(TripleSlot slot);
  
}
