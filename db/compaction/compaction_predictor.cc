// db/compaction/compaction_predictor.cc
#include "db/compaction/compaction_predictor.h"

namespace ROCKSDB_NAMESPACE {

// 由于构造函数在头文件中已定义为内联函数，这里删除构造函数定义
// CompactionPredictor::CompactionPredictor(const VersionStorageInfo* vstorage)
//     : vstorage_(vstorage) {}

std::set<std::string> CompactionPredictor::PredictCompactionFiles() {
  // 不再清空predicted_files_集合，改为创建当前轮次的预测结果集合
  std::set<std::string> current_predicted;
  
  // 特殊处理L0层
  if (vstorage_->CompactionScore(0) > 1.0) {
    // 当L0层的score > 1时，所有L0层的文件都可能进行compaction
    const auto& level_files = vstorage_->LevelFiles(0);
    for (const auto& file : level_files) {
      current_predicted.insert(std::to_string(file->fd.GetNumber()));
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
          current_predicted.insert(std::to_string(l1_file->fd.GetNumber()));
        }
      }
    }
    
    // 更新预测文件的计数
    for (const auto& file : current_predicted) {
      predicted_files_[file]++;
    }
    
    // 将出现次数超过3次的文件从集合中删除
    for (auto it = predicted_files_.begin(); it != predicted_files_.end(); ) {
      if (it->second > 3) {
        it = predicted_files_.erase(it);
      } else {
        ++it;
      }
    }
    
    return current_predicted;
  }
  
  // 检查所有层级的score，找出score > 1的层级
  for (int level = 0; level < vstorage_->num_levels() - 1; level++) {
    if (!CheckLevelScore(level)) continue;
    
    // 对于score > 1的层级，获取可能进行compaction的文件
    auto level_files = GetLevelCompactionFiles(level);
    
    // 移除已知错误预测的文件
    std::set<std::string> filtered_files;
    for (const auto& file : level_files) {
      if (incorrect_predicted_files_.find(file) == incorrect_predicted_files_.end()) {
        filtered_files.insert(file);
      }
    }
    
    // 将该层级的文件添加到当前预测集合中
    current_predicted.insert(filtered_files.begin(), filtered_files.end());
    
    // 计算新的score并检查是否需要继续compaction
    double new_score = CalculateNewScore(level, current_predicted);
    if (new_score > 1.0) {
      // The score is still > 1.0 after removing the files that are already
      // predicted to be compacted. We need to predict more files.
      
      // 尝试找其他可能的起点进行预测
      std::set<std::string> already_predicted;
      for (const auto& file : current_predicted) {
        already_predicted.insert(file);
      }
      
      // 最多重复三次
      for (int i = 0; i < 3 && new_score > 1.0; ++i) {
        // 寻找未被预测过的、最适合作为起点的文件
        auto additional_files = GetNextCompactionFilesFrom(level, already_predicted);
        if (additional_files.empty()) {
          break;  // 没有更多适合的文件了
        }
        
        current_predicted.insert(additional_files.begin(), additional_files.end());
        for (const auto& file : additional_files) {
          already_predicted.insert(file);
        }
        
        new_score = CalculateNewScore(level, current_predicted);
      }
    }
  }
  
  // 检查可能的跨层compaction（例如当前层score不大于1但上层score大于1）
  for (int level = 0; level < vstorage_->num_levels() - 2; level++) {
    if (CheckLevelScore(level)) continue; // 已经处理过
    
    // 检查上层是否有score>1且中间层score>0.8
    for (int upper_level = level + 1; upper_level < vstorage_->num_levels() - 1; upper_level++) {
      if (CheckLevelScore(upper_level) && CheckIntermediateLevelsBetween(upper_level, level)) {
        auto level_files = GetLevelCompactionFiles(level);
        
        // 移除已知错误预测的文件
        std::set<std::string> filtered_files;
        for (const auto& file : level_files) {
          if (incorrect_predicted_files_.find(file) == incorrect_predicted_files_.end()) {
            filtered_files.insert(file);
          }
        }
        
        current_predicted.insert(filtered_files.begin(), filtered_files.end());
        break;
      }
    }
  }
  
  // 更新预测文件的计数
  for (const auto& file : current_predicted) {
    predicted_files_[file]++;
  }
  
  // 将出现次数超过3次的文件从集合中删除
  for (auto it = predicted_files_.begin(); it != predicted_files_.end(); ) {
    if (it->second > 3) {
      it = predicted_files_.erase(it);
    } else {
      ++it;
    }
  }
  
  return current_predicted;
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
  
  if (level_files.empty()) {
    return files;
  }
  
  // 由于VersionStorageInfo没有GetCompactionPointer方法，我们直接选择一个起点文件
  // 如果将来需要获取compaction_pointer，可以通过扩展VersionStorageInfo类添加该方法
  size_t start_index = 0;
  
  // 使用找到的起点文件
  const FileMetaData* start_file = level_files[start_index];
  files.insert(std::to_string(start_file->fd.GetNumber()));
  
  // 找到所有在相同层里键范围重叠的文件
  for (size_t i = 0; i < level_files.size(); i++) {
    if (i == start_index) continue;  // 跳过起点文件自身
    
    const FileMetaData* other_file = level_files[i];
    
    // 检查文件与起点文件是否有重叠
    if (start_file->smallest.user_key().compare(other_file->largest.user_key()) > 0 ||
        start_file->largest.user_key().compare(other_file->smallest.user_key()) < 0) {
      continue;  // 文件没有重叠，跳过
    }
    
    // 文件有重叠，添加到集合中
    uint64_t overlap_file_number = other_file->fd.GetNumber();
    files.insert(std::to_string(overlap_file_number));
  }
  
  // 找到下层与该文件键范围重叠的文件
  if (level + 1 < vstorage_->num_levels()) {
    const Slice& smallest_key = start_file->smallest.user_key();
    const Slice& largest_key = start_file->largest.user_key();
    
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

std::set<std::string> CompactionPredictor::GetNextCompactionFilesFrom(
    int level, const std::set<std::string>& excluded_files) {
  std::set<std::string> files;
  const auto& level_files = vstorage_->LevelFiles(level);
  
  if (level_files.empty()) {
    return files;
  }
  
  // 寻找未被排除的文件且不是已知错误预测的文件作为新的起点
  const FileMetaData* start_file = nullptr;
  for (const auto& file : level_files) {
    std::string file_number = std::to_string(file->fd.GetNumber());
    if (excluded_files.find(file_number) == excluded_files.end() && 
        incorrect_predicted_files_.find(file_number) == incorrect_predicted_files_.end()) {
      start_file = file;
      files.insert(file_number);
      break;
    }
  }
  
  if (start_file == nullptr) {
    // 没有找到合适的起点文件
    return files;
  }
  
  // 找到所有在相同层里键范围重叠的文件
  for (const auto& file : level_files) {
    if (file == start_file) continue;  // 跳过起点文件自身
    
    std::string file_number = std::to_string(file->fd.GetNumber());
    if (excluded_files.find(file_number) != excluded_files.end() ||
        incorrect_predicted_files_.find(file_number) != incorrect_predicted_files_.end()) {
      continue;  // 已被排除或已知错误预测，跳过
    }
    
    // 检查文件与起点文件是否有重叠
    if (start_file->smallest.user_key().compare(file->largest.user_key()) > 0 ||
        start_file->largest.user_key().compare(file->smallest.user_key()) < 0) {
      continue;  // 文件没有重叠，跳过
    }
    
    // 文件有重叠，添加到集合中
    files.insert(file_number);
  }
  
  // 找到下层与该文件键范围重叠的文件
  if (level + 1 < vstorage_->num_levels()) {
    const Slice& smallest_key = start_file->smallest.user_key();
    const Slice& largest_key = start_file->largest.user_key();
    
    // 检查下层是否有重叠的文件
    const auto& next_level_files = vstorage_->LevelFiles(level + 1);
    for (const auto& file : next_level_files) {
      std::string file_number = std::to_string(file->fd.GetNumber());
      if (excluded_files.find(file_number) != excluded_files.end() ||
          incorrect_predicted_files_.find(file_number) != incorrect_predicted_files_.end()) {
        continue;  // 已被排除或已知错误预测，跳过
      }
      
      // 检查文件是否与键范围重叠
      const Slice& file_smallest = file->smallest.user_key();
      const Slice& file_largest = file->largest.user_key();
      
      if (!(smallest_key.compare(file_largest) > 0 ||
            largest_key.compare(file_smallest) < 0)) {
        // 文件与键范围重叠，添加到集合中
        files.insert(file_number);
      }
    }
  }
  
  return files;
}

void CompactionPredictor::RemoveIncorrectPredictedFiles(
    const std::set<std::string>& incorrect_files) {
  // 从预测集合中删除不正确的文件
  for (const auto& file : incorrect_files) {
    predicted_files_.erase(file);
  }
  
  // 记录这些不正确的文件，以便以后的预测可以避免
  incorrect_predicted_files_.insert(incorrect_files.begin(), incorrect_files.end());
}

} // namespace ROCKSDB_NAMESPACE