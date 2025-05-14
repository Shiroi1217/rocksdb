// db/compaction/compaction_predictor.cc
#include "db/compaction/compaction_predictor.h"
#include "logging/logging.h"

namespace ROCKSDB_NAMESPACE {

// 由于构造函数在头文件中已定义为内联函数，这里删除构造函数定义
// CompactionPredictor::CompactionPredictor(const VersionStorageInfo* vstorage)
//     : vstorage_(vstorage) {}

std::set<std::string> CompactionPredictor::PredictCompactionFiles() {
  // 不再清空predicted_files_集合，改为创建当前轮次的预测结果集合
  std::set<std::string> current_predicted;
  
  // 特殊处理L0层 - 但根据需求，跳过L0层的预测
  if (vstorage_->CompactionScore(0) > 1.0) {
    // L0层不进行预测，因为L0层的文件选择逻辑较为特殊，预测准确率低
    ROCKS_LOG_INFO(nullptr, "跳过L0层预测，因为L0层文件选择逻辑特殊");
    // 注意：不再直接返回，继续检查其他层级
  }
  
  // 检查所有层级的score，找出score > 1的层级
  for (int level = 1; level < vstorage_->num_levels() - 1; level++) {
    if (!CheckLevelScore(level)) continue;
    
    // 对于score > 1的层级，获取可能进行compaction的文件
    auto level_files = GetLevelCompactionFiles(level);
    
    if (level_files.empty()) {
      ROCKS_LOG_INFO(nullptr, "Level %d 未找到适合compaction的文件", level);
      continue;
    }
    
    ROCKS_LOG_INFO(nullptr, "Level %d 预测到 %zu 个文件将进行compaction", 
                  level, level_files.size());
    
    // 移除已知错误预测的文件
    std::set<std::string> filtered_files;
    for (const auto& file : level_files) {
      if (incorrect_predicted_files_.find(file) == incorrect_predicted_files_.end()) {
        filtered_files.insert(file);
      }
    }
    
    if (filtered_files.size() < level_files.size()) {
      ROCKS_LOG_INFO(nullptr, "Level %d 移除了 %zu 个已知错误预测的文件", 
                    level, level_files.size() - filtered_files.size());
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
      
      ROCKS_LOG_INFO(nullptr, "Level %d 在移除预测文件后Score仍为 %.2f，继续预测", 
                    level, new_score);
      
      // 最多重复三次
      for (int i = 0; i < 3 && new_score > 1.0; ++i) {
        // 寻找未被预测过的、最适合作为起点的文件
        auto additional_files = GetNextCompactionFilesFrom(level, already_predicted);
        if (additional_files.empty()) {
          ROCKS_LOG_INFO(nullptr, "Level %d 未找到更多适合预测的文件，停止预测", level);
          break;  // 没有更多适合的文件了
        }
        
        ROCKS_LOG_INFO(nullptr, "Level %d 第%d轮额外预测到 %zu 个文件", 
                      level, i+1, additional_files.size());
        
        current_predicted.insert(additional_files.begin(), additional_files.end());
        for (const auto& file : additional_files) {
          already_predicted.insert(file);
        }
        
        new_score = CalculateNewScore(level, current_predicted);
        ROCKS_LOG_INFO(nullptr, "Level %d 第%d轮预测后Score为 %.2f", 
                      level, i+1, new_score);
      }
    }
  }
  
  // 检查可能的跨层compaction（例如当前层score不大于1但上层score大于1）
  for (int level = 1; level < vstorage_->num_levels() - 2; level++) {
    if (CheckLevelScore(level)) continue; // 已经处理过
    
    // 检查上层是否有score>1且中间层score>0.8
    for (int upper_level = level + 1; upper_level < vstorage_->num_levels() - 1; upper_level++) {
      if (CheckLevelScore(upper_level) && CheckIntermediateLevelsBetween(upper_level, level)) {
        ROCKS_LOG_INFO(nullptr, "检测到跨层compaction: Level %d -> Level %d", 
                      upper_level, level);
        
        auto level_files = GetLevelCompactionFiles(level);
        
        // 移除已知错误预测的文件
        std::set<std::string> filtered_files;
        for (const auto& file : level_files) {
          if (incorrect_predicted_files_.find(file) == incorrect_predicted_files_.end()) {
            filtered_files.insert(file);
          }
        }
        
        ROCKS_LOG_INFO(nullptr, "跨层compaction: Level %d 预测到 %zu 个文件", 
                      level, filtered_files.size());
        
        current_predicted.insert(filtered_files.begin(), filtered_files.end());
        break;
      }
    }
  }
  
  // 更新预测文件的计数
  for (const auto& file : current_predicted) {
    predicted_files_[file]++;
    ROCKS_LOG_INFO(nullptr, "文件 %s 被预测次数: %d", 
                  file.c_str(), predicted_files_[file]);
  }
  
  // 将出现次数超过3次的文件从集合中删除
  for (auto it = predicted_files_.begin(); it != predicted_files_.end(); ) {
    if (it->second > 3) {
      ROCKS_LOG_INFO(nullptr, "文件 %s 被预测超过3次，从预测集合中移除", 
                    it->first.c_str());
      it = predicted_files_.erase(it);
    } else {
      ++it;
    }
  }
  
  ROCKS_LOG_INFO(nullptr, "本轮预测完成，共预测到 %zu 个文件", current_predicted.size());
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
  
  // 使用VersionStorageInfo的NextCompactionIndex来找出最可能进行compaction的文件
  // 这与CompactionPicker的选择逻辑一致，提高预测准确率
  size_t start_index = 0;
  // 如果level > 0，尝试使用VersionStorageInfo的NextCompactionIndex
  if (level > 0) {
    start_index = vstorage_->NextCompactionIndex(level);
    if (start_index >= level_files.size()) {
      start_index = 0; // 防止索引越界
    }
  }
  
  // 使用找到的起点文件
  const FileMetaData* start_file = level_files[start_index];
  std::string start_file_number = std::to_string(start_file->fd.GetNumber());
  files.insert(start_file_number);
  
  ROCKS_LOG_INFO(nullptr, "Level %d 选择文件 %s 作为预测起点，键范围: [%s, %s]", 
                level, start_file_number.c_str(),
                start_file->smallest.DebugString(true).c_str(),
                start_file->largest.DebugString(true).c_str());
  
  // 找到所有在相同层里键范围重叠的文件
  for (size_t i = 0; i < level_files.size(); i++) {
    if (i == start_index) continue;  // 跳过起点文件自身
    
    const FileMetaData* other_file = level_files[i];
    std::string other_file_number = std::to_string(other_file->fd.GetNumber());
    
    // 检查文件与起点文件是否有重叠
    if (start_file->smallest.user_key().compare(other_file->largest.user_key()) > 0 ||
        start_file->largest.user_key().compare(other_file->smallest.user_key()) < 0) {
      continue;  // 文件没有重叠，跳过
    }
    
    // 文件有重叠，添加到集合中
    files.insert(other_file_number);
    ROCKS_LOG_INFO(nullptr, "Level %d 文件 %s 与起点文件键范围重叠，添加到预测集合", 
                  level, other_file_number.c_str());
  }
  
  // 找到下层与该文件键范围重叠的文件
  if (level + 1 < vstorage_->num_levels()) {
    const Slice& smallest_key = start_file->smallest.user_key();
    const Slice& largest_key = start_file->largest.user_key();
    
    // 检查下层是否有重叠的文件
    const auto& next_level_files = vstorage_->LevelFiles(level + 1);
    ROCKS_LOG_INFO(nullptr, "检查Level %d 的 %zu 个文件是否与键范围重叠", 
                  level + 1, next_level_files.size());
    
    int overlap_count = 0;
    for (const auto& file : next_level_files) {
      // 检查文件是否与键范围重叠
      const Slice& file_smallest = file->smallest.user_key();
      const Slice& file_largest = file->largest.user_key();
      
      if (!(smallest_key.compare(file_largest) > 0 ||
            largest_key.compare(file_smallest) < 0)) {
        // 文件与键范围重叠，添加到集合中
        std::string file_number = std::to_string(file->fd.GetNumber());
        files.insert(file_number);
        overlap_count++;
        ROCKS_LOG_INFO(nullptr, "Level %d 文件 %s 与上层键范围重叠，添加到预测集合", 
                      level + 1, file_number.c_str());
      }
    }
    
    if (overlap_count == 0) {
      ROCKS_LOG_INFO(nullptr, "Level %d 未找到与上层键范围重叠的文件", level + 1);
      // 如果预测没有找到下层重叠文件，可能是预测逻辑与实际compaction逻辑不同
      // 这种情况下，将整个层级的文件都加入预测集合
      ROCKS_LOG_INFO(nullptr, "预测失败，将Level %d 的所有 %zu 个文件加入预测集合", 
                    level, level_files.size());
      for (const auto& file : level_files) {
        std::string file_number = std::to_string(file->fd.GetNumber());
        files.insert(file_number);
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
    auto it = predicted_files_.find(file);
    if (it != predicted_files_.end()) {
      ROCKS_LOG_INFO(nullptr, "文件 %s 已被compaction，从预测集合中移除", file.c_str());
      predicted_files_.erase(it);
    }
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
    auto it = predicted_files_.find(file);
    if (it != predicted_files_.end()) {
      ROCKS_LOG_INFO(nullptr, "文件 %s 是错误预测，从预测集合中移除", file.c_str());
      predicted_files_.erase(it);
    }
  }
  
  // 记录这些不正确的文件，以便以后的预测可以避免
  incorrect_predicted_files_.insert(incorrect_files.begin(), incorrect_files.end());
}

} // namespace ROCKSDB_NAMESPACE