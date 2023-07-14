/**
 * index_iterator.cpp
 */
#include <cassert>

#include "common/config.h"
#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return page_ == nullptr; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  mapping_ = MappingType{page_->KeyAt(index_), page_->ValueAt(index_)};
  return mapping_;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (page_ == nullptr) {
    return *this;
  }

  ++index_;
  if (index_ == page_->GetSize()) {
    index_ = 0;
    if (page_->GetNextPageId() == INVALID_PAGE_ID) {
      guard_ = nullptr;
      page_ = nullptr;
    } else {
      page_id_t next_page_id = page_->GetNextPageId();
      guard_ = std::make_unique<BasicPageGuard>(bpm_->FetchPageBasic(next_page_id));
      page_ = const_cast<LeafPage *>(guard_->As<LeafPage>());
    }
  }

  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
