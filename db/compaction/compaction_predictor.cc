// db/compaction/compaction_predictor.cc
#include "db/compaction/compaction_predictor.h"

namespace ROCKSDB_NAMESPACE {

// 由于构造函数在头文件中已定义为内联函数，这里删除构造函数定义
// CompactionPredictor::CompactionPredictor(const VersionStorageInfo* vstorage)
//     : vstorage_(vstorage) {}

std::set<std::string> CompactionPredictor::PredictCompactionFiles() {
  predicted_files_.clear();
  
  // 特殊处理L0层
  if (vstorage_->CompactionScore(0) > 1.0) {
    // 当L0层的score > 1时，所有L0层的文件都可能进行compaction
    const auto& level_files = vstorage_->LevelFiles(0);
    for (const auto& file : level_files) {
      predicted_files_.insert(std::to_string(file->fd.GetNumber()));
    }
    
    // 可能的话，找出L1层可能会与L0文件重叠的文件
    if (vstorage_->num_levels() > 1 && !level_files.empty()) {
      const auto& l1_files = vstorage_->LevelFiles(1);
      for (const auto& l1_file : l1_files) {
        // 检查L1文件是否与任何L0文件有重叠
        bool has_overlap = false;
        for (const auto& l0_file : level_files) {
          // 检查键范围是否有重叠
          if (!(l1_file->smallest.user_key().compare(l0_file->largest.user_key()) > 0 ||
                l1_file->largest.user_key().compare(l0_file->smallest.user_key()) < 0)) {
            has_overlap = true;
            break;
          }
        }
        
        if (has_overlap) {
          predicted_files_.insert(std::to_string(l1_file->fd.GetNumber()));
        }
      }
    }
    
    return predicted_files_;
  }
  
  // 从L1开始遍历到最大层
  for (int level = 1; level < vstorage_->num_levels(); ++level) {
    // 检查当前层score是否>1
    if (CheckLevelScore(level)) {
      auto level_files = GetLevelCompactionFiles(level);
      predicted_files_.insert(level_files.begin(), level_files.end());
      continue;
    }
    
    // 检查上层是否有score>1且中间层score>0.8
    for (int upper_level = level + 1; upper_level < vstorage_->num_levels(); ++upper_level) {
      if (CheckLevelScore(upper_level) && CheckIntermediateLevelsBetween(upper_level, level)) {
        auto level_files = GetLevelCompactionFiles(level);
        predicted_files_.insert(level_files.begin(), level_files.end());
        break;
      }
    }
    
    // 计算新的score并检查是否需要继续compaction
    double new_score = CalculateNewScore(level, predicted_files_);
    if (new_score > 1.0) {
      // The score is still > 1.0 after removing the files that are already
      // predicted to be compacted. We need to predict more files.
      
      // 最多重复三次
      for (int i = 0; i < 3; ++i) {
        auto additional_files = GetLevelCompactionFiles(level);
        predicted_files_.insert(additional_files.begin(), additional_files.end());
        
        new_score = CalculateNewScore(level, predicted_files_);
        if (new_score <= 1.0) break;
      }
    }
  }
  
  return predicted_files_;
}

// CheckLevelScore已在头文件中定义为内联函数，无需重复定义

// 检查中间层级的得分，如果中间层的得分都大于0.8，则返回true
bool CompactionPredictor::CheckIntermediateLevelsBetween(int start_level, int target_level) {
  for (int level = start_level - 1; level > target_level; --level) {
    if (vstorage_->CompactionScore(level) <= 0.8) {
      return false;
    }
  }
  return true;
}

std::set<std::string> CompactionPredictor::GetLevelCompactionFiles(int level) {
  std::set<std::string> files;
  const auto& level_files = vstorage_->LevelFiles(level);
  
  // 获取compaction指针指向的文件
  if (!level_files.empty()) {
    uint64_t file_number = level_files[0]->fd.GetNumber();
    files.insert(std::to_string(file_number));
    
    // 找到所有在相同层里键范围重叠的文件
    for (size_t i = 1; i < level_files.size(); i++) {
      // 检查文件与第一个文件是否有键范围重叠
      const FileMetaData* f1 = level_files[0];
      const FileMetaData* f2 = level_files[i];
      
      // 检查f1和f2是否有重叠
      if (f1->smallest.user_key().compare(f2->largest.user_key()) > 0 ||
          f1->largest.user_key().compare(f2->smallest.user_key()) < 0) {
        continue;  // 文件没有重叠，跳过
      }
      
      // 文件有重叠，添加到集合中
      uint64_t overlap_file_number = level_files[i]->fd.GetNumber();
      files.insert(std::to_string(overlap_file_number));
    }
    
    // 找到下层与该文件键范围重叠的文件
    if (level + 1 < vstorage_->num_levels()) {
      const Slice& smallest_key = level_files[0]->smallest.user_key();
      const Slice& largest_key = level_files[0]->largest.user_key();
      
      // 检查下层是否有重叠的文件
      const auto& next_level_files = vstorage_->LevelFiles(level + 1);
      for (const auto& file : next_level_files) {
        // 检查文件是否与键范围重叠
        const Slice& file_smallest = file->smallest.user_key();
        const Slice& file_largest = file->largest.user_key();
        
        if (!(smallest_key.compare(file_largest) > 0 ||
              largest_key.compare(file_smallest) < 0)) {
          // 文件与键范围重叠，添加到集合中
          uint64_t overlap_file_number = file->fd.GetNumber();
          files.insert(std::to_string(overlap_file_number));
        }
      }
    }
  }
  
  return files;
}

double CompactionPredictor::CalculateNewScore(int level, const std::set<std::string>& files_to_remove) {
  if (level < 1 || level >= vstorage_->num_levels()) {
    return 0.0;
  }
  
  // 不计算0层的score值，L0层用文件数量直接判断
  // 对于其他层级，score是基于文件大小计算的
  uint64_t level_size = vstorage_->NumLevelBytes(level);
  uint64_t removed_size = 0;
  
  // 计算要移除的文件的总大小
  const auto& level_files = vstorage_->LevelFiles(level);
  for (const auto& file : level_files) {
    if (files_to_remove.find(std::to_string(file->fd.GetNumber())) != files_to_remove.end()) {
      removed_size += file->fd.GetFileSize();
    }
  }
  
  // 如果移除的文件大小超过了层级大小，说明有错误
  if (removed_size > level_size) {
    return 0.0;
  }
  
  // 计算移除文件后的层级大小
  uint64_t new_level_size = level_size - removed_size;
  
  // 计算当前层级得分与大小的比率，以便计算新的得分
  double score_per_byte = 0.0;
  if (level_size > 0) {
    score_per_byte = vstorage_->CompactionScore(level) / static_cast<double>(level_size);
  }
  
  // 使用新的大小计算得分
  double current_score = score_per_byte * static_cast<double>(new_level_size);
  
  return current_score;
}

bool CompactionPredictor::KeysInRangeOverlapWithFile(int level, const Slice& smallest_key,
                                          const Slice& largest_key,
                                          const std::string& file_number) {
  const auto& level_files = vstorage_->LevelFiles(level);
  for (const auto& file : level_files) {
    if (std::to_string(file->fd.GetNumber()) == file_number) {
      // 检查键范围是否有重叠
      if (!(smallest_key.compare(file->largest.user_key()) > 0 ||
            largest_key.compare(file->smallest.user_key()) < 0)) {
        return true;
      }
    }
  }
  return false;
}

// 检查两个键范围是否在指定层有顺序相关性
bool CompactionPredictor::Before(int /*level*/, const Slice& /*smallest_key*/,
                                const Slice& largest_key,
                                const Slice& file_smallest, 
                                const Slice& /*file_largest*/) {
  // 如果键范围没有重叠且最大键小于文件的最小键，则说明在该文件之前
  return largest_key.compare(file_smallest) < 0;
}

// 从预测集合中删除已经被compaction的文件
void CompactionPredictor::RemoveCompactedFiles(const std::set<std::string>& compacted_files) {
  for (const auto& file : compacted_files) {
    predicted_files_.erase(file);
  }
}

} // namespace ROCKSDB_NAMESPACE