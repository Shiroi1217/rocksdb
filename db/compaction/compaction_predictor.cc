// db/compaction/compaction_predictor.cc
#include "db/compaction/compaction_predictor.h"
#include "logging/logging.h"

namespace ROCKSDB_NAMESPACE {

// 由于构造函数在头文件中已定义为内联函数，这里删除构造函数定义
// CompactionPredictor::CompactionPredictor(const VersionStorageInfo* vstorage)
//     : vstorage_(vstorage) {}

std::set<std::string> CompactionPredictor::PredictCompactionFiles() {
  // 清空predicted_files_集合，存储本次预测结果
  std::set<std::string> current_predicted;
  
  // 确保我们获取到最新的层级信息和score值
  ROCKS_LOG_INFO(nullptr, "开始预测下一轮compaction文件，获取最新层级信息...");
  
  // 输出每一层的score值、文件数量和大小进行调试验证
  for (int level = 0; level < vstorage_->num_levels() - 1; level++) {
    double score = vstorage_->CompactionScore(level);
    int file_count = vstorage_->NumLevelFiles(level);
    uint64_t level_size = vstorage_->NumLevelBytes(level);
    
    ROCKS_LOG_INFO(nullptr, "Level %d 的compaction score: %.2f, 文件数: %d, 总大小: %llu bytes",
                 level, score, file_count, static_cast<unsigned long long>(level_size));
  }
  
  // 记录每个层级的score和compaction优先级
  int highest_score_level = vstorage_->CompactionScoreLevel(0);
  double highest_score = vstorage_->CompactionScore(highest_score_level);
  
  ROCKS_LOG_INFO(nullptr, "当前最高优先级的层级是 Level %d, score: %.2f", 
                highest_score_level, highest_score);
  
  // 1. 首先处理L0到L1的预测
  if (vstorage_->NumLevelFiles(0) > 0) {
    double l0_score = vstorage_->CompactionScore(0);
    if (l0_score > 1.0) {
      ROCKS_LOG_INFO(nullptr, "检测到L0有 %d 个文件, score为 %.2f > 1.0, 预测L0到L1 compaction",
                    vstorage_->NumLevelFiles(0), l0_score);
      
      // 对于L0到L1的compaction，我们只预测L1层的文件
      auto l1_files = GetPossibleTargetFilesForL0Compaction();
      
      if (!l1_files.empty()) {
        ROCKS_LOG_INFO(nullptr, "预测到L0到L1 compaction可能会影响 %zu 个L1文件", 
                      l1_files.size());
        
        // 将所有L1文件添加到预测集合中
        current_predicted.insert(l1_files.begin(), l1_files.end());
      } else {
        ROCKS_LOG_INFO(nullptr, "未找到可能受L0 compaction影响的L1文件");
      }
    } else {
      ROCKS_LOG_INFO(nullptr, "L0 score为 %.2f <= 1.0, 不会触发L0->L1 compaction", l0_score);
    }
  }
  
  // 2. 处理非L0层级的预测
  // 分两种情况：直接满足score > 1的层级，以及由于上层score > 1且中间层都有较高score导致的compaction
  for (int level = 1; level < vstorage_->num_levels() - 1; level++) {
    // 跳过空层级
    if (vstorage_->NumLevelFiles(level) == 0) {
      continue;
    }
    
    double level_score = vstorage_->CompactionScore(level);
    bool will_compact = false;
    std::string reason;
    
    // 情况1: 当前层score > 1，直接触发compaction
    if (level_score > 1.0) {
      will_compact = true;
      reason = "score > 1.0";
    } 
    // 情况2: 检查上层是否有score > 1且中间层score都 > 0.8
    else {
      bool higher_level_over_threshold = false;
      bool all_intermediate_level_ok = true;
      int triggering_level = -1;
      
      // 从上一层往上检查，是否有score > 1的层级
      for (int higher_level = level - 1; higher_level >= 0; higher_level--) {
        double higher_score = vstorage_->CompactionScore(higher_level);
        if (higher_score > 1.0) {
          higher_level_over_threshold = true;
          triggering_level = higher_level;
          
          // 检查中间层级的score是否都 > 0.8
          for (int mid_level = higher_level + 1; mid_level < level; mid_level++) {
            if (vstorage_->CompactionScore(mid_level) <= 0.8) {
              all_intermediate_level_ok = false;
              break;
            }
          }
          
          break;
        }
      }
      
      // 上层有score > 1且中间层都score > 0.8
      if (higher_level_over_threshold && all_intermediate_level_ok) {
        will_compact = true;
        reason = "上层L" + std::to_string(triggering_level) + " score > 1.0且中间层score都 > 0.8";
      }
      // 特殊情况: 检查L1到L2的特殊compaction情况
      else if (level == 1 && CheckL1ToL2Compaction()) {
        will_compact = true;
        reason = "L0积压导致的特殊L1->L2 compaction";
      }
    }
    
    // 如果该层可能会发生compaction，预测相关文件
    if (will_compact) {
      ROCKS_LOG_INFO(nullptr, "预测Level %d compaction文件，原因: %s", level, reason.c_str());
      
      // 获取该层级可能进行compaction的文件
      auto level_files = GetLevelCompactionFiles(level);
      
      if (!level_files.empty()) {
        ROCKS_LOG_INFO(nullptr, "Level %d 预测到 %zu 个文件将进行compaction", 
                      level, level_files.size());
        
        // 将该层级的文件添加到当前预测集合中
        current_predicted.insert(level_files.begin(), level_files.end());
        
        // 计算新的score并检查是否需要继续预测
        double new_score = CalculateNewScore(level, level_files);
        ROCKS_LOG_INFO(nullptr, "移除预测文件后Level %d 的新score: %.2f", level, new_score);
        
        // 如果新score仍然 > 1.0，继续预测
        if (new_score > 1.0) {
          // 最多重复预测三次
          std::set<std::string> already_predicted = level_files;
          for (int i = 0; i < 3 && new_score > 1.0; ++i) {
            ROCKS_LOG_INFO(nullptr, "第%d轮额外预测: 移除已预测文件后score仍为 %.2f > 1.0", 
                          i+1, new_score);
            
            // 寻找未被预测过的、最适合作为起点的文件
            auto additional_files = GetNextCompactionFilesFrom(level, already_predicted);
            if (additional_files.empty()) {
              ROCKS_LOG_INFO(nullptr, "未找到更多适合预测的文件，停止预测");
              break;
            }
            
            ROCKS_LOG_INFO(nullptr, "额外预测到 %zu 个Level %d 文件", 
                          additional_files.size(), level);
            
            // 添加到预测集和已处理集合
            current_predicted.insert(additional_files.begin(), additional_files.end());
            for (const auto& file : additional_files) {
              already_predicted.insert(file);
            }
            
            // 重新计算score
            new_score = CalculateNewScore(level, already_predicted);
            ROCKS_LOG_INFO(nullptr, "移除所有预测文件后Level %d 的新score: %.2f", 
                          level, new_score);
          }
        }
        
        // 查找目标层文件（当前层+1）的重叠文件
        if (level + 1 < vstorage_->num_levels()) {
          auto target_level_files = GetTargetLevelFilesForCompaction(level, level + 1, level_files);
          
          if (!target_level_files.empty()) {
            ROCKS_LOG_INFO(nullptr, "预测到Level %d 到 Level %d compaction会涉及 %zu 个Level %d文件", 
                         level, level + 1, target_level_files.size(), level + 1);
            
            // 添加目标层文件到预测集合
            current_predicted.insert(target_level_files.begin(), target_level_files.end());
          } else {
            ROCKS_LOG_INFO(nullptr, "未找到与Level %d 文件键范围重叠的Level %d 文件", 
                          level, level + 1);
          }
        }
      } else {
        ROCKS_LOG_INFO(nullptr, "Level %d 未找到适合compaction的文件", level);
      }
    } else {
      ROCKS_LOG_INFO(nullptr, "Level %d 不会触发compaction (score: %.2f)", level, level_score);
    }
  }
  
  // 更新预测次数并输出信息
  for (const auto& file : current_predicted) {
    predicted_files_[file]++;
    ROCKS_LOG_INFO(nullptr, "文件 %s 被预测次数: %d", 
                  file.c_str(), predicted_files_[file]);
  }
  
  if (current_predicted.empty()) {
    ROCKS_LOG_INFO(nullptr, "未能预测到下一轮compaction的文件");
  } else {
    ROCKS_LOG_INFO(nullptr, "本轮预测完成，共预测到 %zu 个文件", current_predicted.size());
  }
  
  return current_predicted;
}

// 获取L0到L1 compaction可能涉及的L1文件
std::set<std::string> CompactionPredictor::GetPossibleTargetFilesForL0Compaction() {
  std::set<std::string> l1_files;
  
  // 检查L0和L1是否有文件
  if (vstorage_->NumLevelFiles(0) == 0 || vstorage_->NumLevelFiles(1) == 0) {
    ROCKS_LOG_INFO(nullptr, "L0层或L1层没有文件，无需预测L0到L1 compaction");
    return l1_files;
  }
  
  // 获取L0和L1层的所有文件
  const auto& l0_files_vec = vstorage_->LevelFiles(0);
  const auto& l1_files_vec = vstorage_->LevelFiles(1);
  
  // 确定L0文件的整体键范围
  Slice smallest_l0_key;
  Slice largest_l0_key;
  bool initialized = false;
  
  // 计算L0层所有未被compaction的文件的键范围并合并
  for (const auto& file : l0_files_vec) {
    if (file->being_compacted) {
      ROCKS_LOG_INFO(nullptr, "L0文件 %lu 正在compaction中，跳过", file->fd.GetNumber());
      continue;
    }
    
    if (!initialized) {
      smallest_l0_key = file->smallest.user_key();
      largest_l0_key = file->largest.user_key();
      initialized = true;
    } else {
      // 更新最小键和最大键
      if (smallest_l0_key.compare(file->smallest.user_key()) > 0) {
        smallest_l0_key = file->smallest.user_key();
      }
      if (largest_l0_key.compare(file->largest.user_key()) < 0) {
        largest_l0_key = file->largest.user_key();
      }
    }
    
    ROCKS_LOG_INFO(nullptr, "L0文件 %lu 键范围: [%s, %s]", 
                  file->fd.GetNumber(),
                  file->smallest.DebugString(true).c_str(),
                  file->largest.DebugString(true).c_str());
  }
  
  // 如果找不到有效的L0文件范围，返回空集合
  if (!initialized) {
    ROCKS_LOG_INFO(nullptr, "没有找到可用于compaction的L0文件");
    return l1_files;
  }
  
  ROCKS_LOG_INFO(nullptr, "L0层文件整体键范围: [%s, %s]", 
                smallest_l0_key.ToString(true).c_str(),
                largest_l0_key.ToString(true).c_str());
  
  // 寻找与L0文件键范围重叠的L1文件
  int overlapping_files_count = 0;
  
  for (const auto& file : l1_files_vec) {
    if (file->being_compacted) {
      ROCKS_LOG_INFO(nullptr, "L1文件 %lu 正在compaction中，跳过", file->fd.GetNumber());
      continue;
    }
    
    Slice file_smallest = file->smallest.user_key();
    Slice file_largest = file->largest.user_key();
    
    // 检查是否与L0整体范围重叠
    bool overlaps = !(file_largest.compare(smallest_l0_key) < 0 || 
                     file_smallest.compare(largest_l0_key) > 0);
                     
    if (overlaps) {
      std::string file_number = std::to_string(file->fd.GetNumber());
      l1_files.insert(file_number);
      overlapping_files_count++;
      
      ROCKS_LOG_INFO(nullptr, "L1文件 %s 与L0键范围重叠，添加到预测集合，键范围: [%s, %s]",
                    file_number.c_str(),
                    file->smallest.DebugString(true).c_str(),
                    file->largest.DebugString(true).c_str());
    } else {
      ROCKS_LOG_INFO(nullptr, "L1文件 %lu 与L0键范围不重叠，键范围: [%s, %s]",
                    file->fd.GetNumber(),
                    file->smallest.DebugString(true).c_str(),
                    file->largest.DebugString(true).c_str());
    }
  }
  
  ROCKS_LOG_INFO(nullptr, "总共找到 %d 个与L0层键范围重叠的L1文件", overlapping_files_count);
  
  return l1_files;
}

// 获取与源层键范围重叠的目标层文件
std::set<std::string> CompactionPredictor::GetTargetLevelFilesForCompaction(
    int source_level, int target_level, const std::set<std::string>& source_files) {
  std::set<std::string> target_files;
  
  // 参数检查
  if (source_files.empty()) {
    ROCKS_LOG_INFO(nullptr, "源层 Level %d 没有待处理的文件", source_level);
    return target_files;
  }
  
  if (target_level >= vstorage_->num_levels()) {
    ROCKS_LOG_INFO(nullptr, "目标层 Level %d 超出了最大层级 %d", 
                   target_level, vstorage_->num_levels() - 1);
    return target_files;
  }
  
  // 检查目标层是否有文件
  if (vstorage_->NumLevelFiles(target_level) == 0) {
    ROCKS_LOG_INFO(nullptr, "目标层 Level %d 没有文件", target_level);
    return target_files;
  }
  
  // 从源文件字符串获取FileMetaData指针
  std::vector<FileMetaData*> source_file_meta;
  const auto& source_level_files = vstorage_->LevelFiles(source_level);
  
  ROCKS_LOG_INFO(nullptr, "Level %d 有 %d 个文件，其中 %zu 个文件被选为源文件", 
                source_level, static_cast<int>(source_level_files.size()), source_files.size());
  
  // 收集源文件的元数据
  for (const auto& file : source_level_files) {
    std::string file_number = std::to_string(file->fd.GetNumber());
    if (source_files.find(file_number) != source_files.end()) {
      if (file->being_compacted) {
        ROCKS_LOG_INFO(nullptr, "源层 Level %d 文件 %s 正在compaction，跳过", 
                     source_level, file_number.c_str());
        continue;
      }
      source_file_meta.push_back(file);
      ROCKS_LOG_INFO(nullptr, "添加源层 Level %d 文件 %s 到处理列表，键范围: [%s, %s]",
                   source_level, file_number.c_str(),
                   file->smallest.DebugString(true).c_str(),
                   file->largest.DebugString(true).c_str());
    }
  }
  
  if (source_file_meta.empty()) {
    ROCKS_LOG_INFO(nullptr, "源层 Level %d 没有可用于重叠检测的文件", source_level);
    return target_files;
  }
  
  // 计算源文件的整体键范围
  Slice smallest_source_key = source_file_meta[0]->smallest.user_key();
  Slice largest_source_key = source_file_meta[0]->largest.user_key();
  
  for (size_t i = 1; i < source_file_meta.size(); i++) {
    if (smallest_source_key.compare(source_file_meta[i]->smallest.user_key()) > 0) {
      smallest_source_key = source_file_meta[i]->smallest.user_key();
    }
    if (largest_source_key.compare(source_file_meta[i]->largest.user_key()) < 0) {
      largest_source_key = source_file_meta[i]->largest.user_key();
    }
  }
  
  ROCKS_LOG_INFO(nullptr, "源层 Level %d 文件整体键范围: [%s, %s]", 
                source_level,
                smallest_source_key.ToString(true).c_str(),
                largest_source_key.ToString(true).c_str());
  
  // 寻找与源层文件键范围重叠的目标层文件
  const auto& target_level_files = vstorage_->LevelFiles(target_level);
  int overlapping_count = 0;
  
  ROCKS_LOG_INFO(nullptr, "目标层 Level %d 有 %d 个文件", 
                target_level, static_cast<int>(target_level_files.size()));
  
  for (const auto& file : target_level_files) {
    if (file->being_compacted) {
      ROCKS_LOG_INFO(nullptr, "目标层 Level %d 文件 %lu 正在compaction，跳过", 
                     target_level, file->fd.GetNumber());
      continue;
    }
    
    Slice file_smallest = file->smallest.user_key();
    Slice file_largest = file->largest.user_key();
    
    // 检查是否与源层整体范围重叠
    bool overlaps = !(file_largest.compare(smallest_source_key) < 0 ||
                      file_smallest.compare(largest_source_key) > 0);
    
    std::string file_number = std::to_string(file->fd.GetNumber());
    
    if (overlaps) {
      target_files.insert(file_number);
      overlapping_count++;
      
      ROCKS_LOG_INFO(nullptr, "目标层 Level %d 文件 %s 与源层键范围重叠，添加到预测集合，键范围: [%s, %s]",
                    target_level, file_number.c_str(),
                    file->smallest.DebugString(true).c_str(),
                    file->largest.DebugString(true).c_str());
    } else {
      // 仅在调试级别记录不重叠的文件
      ROCKS_LOG_DEBUG(nullptr, "目标层 Level %d 文件 %s 与源层键范围不重叠，键范围: [%s, %s]",
                     target_level, file_number.c_str(),
                     file->smallest.DebugString(true).c_str(),
                     file->largest.DebugString(true).c_str());
    }
  }
  
  ROCKS_LOG_INFO(nullptr, "总共找到 %d 个与源层 Level %d 键范围重叠的目标层 Level %d 文件", 
                overlapping_count, source_level, target_level);
  
  return target_files;
}

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
    ROCKS_LOG_INFO(nullptr, "Level %d 没有文件，无法预测compaction", level);
    return files;
  }
  
  ROCKS_LOG_INFO(nullptr, "开始预测Level %d 的compaction文件，当前层有 %d 个文件", 
                level, static_cast<int>(level_files.size()));
  
  // 查询是否有已经标记为compaction的文件
  int marked_count = 0;
  for (const auto& file : level_files) {
    if (file->being_compacted) {
      marked_count++;
    }
  }
  ROCKS_LOG_INFO(nullptr, "Level %d 有 %d 个文件已被标记为compaction", level, marked_count);
  
  // 对于L0层，逻辑不同，通常是选择最老的文件
  if (level == 0) {
    ROCKS_LOG_INFO(nullptr, "L0层使用特殊的文件选择逻辑，选择最老的文件作为起点");
    
    // 由于L0层文件互相可能有重叠，我们只预测下一层（L1）与L0层重叠的文件
    // 对于L0层本身，不做预测，因为通常是整层一起进行合并
    ROCKS_LOG_INFO(nullptr, "L0层不预测具体文件，请使用GetPossibleTargetFilesForL0Compaction方法预测L1层文件");
    return files; // 对L0层不做预测，由专门的函数处理
  } 
  
  // 对于非L0层级，使用compaction_pri来选择文件
  const std::vector<int>& file_by_compaction_pri = vstorage_->FilesByCompactionPri(level);
  if (file_by_compaction_pri.empty()) {
    ROCKS_LOG_INFO(nullptr, "Level %d 没有按压缩优先级排序的文件", level);
    return files;
  }
  
  // 获取当前层的下一个compaction index
  unsigned int compaction_pointer_index = vstorage_->NextCompactionIndex(level);
  ROCKS_LOG_INFO(nullptr, "Level %d 的当前NextCompactionIndex: %u", level, compaction_pointer_index);
  
  // 如果已经到达文件列表末尾，重置到开始位置
  if (compaction_pointer_index >= file_by_compaction_pri.size()) {
    compaction_pointer_index = 0;
    ROCKS_LOG_INFO(nullptr, "Level %d 的NextCompactionIndex已超出范围，重置为0", level);
  }
  
  // 寻找合适的起始文件
  bool found_start_file = false;
  size_t checked_files = 0;
  int start_index = -1;
  FileMetaData* start_file = nullptr;
  
  // 从compaction pointer位置开始寻找第一个未被compaction的文件
  while (checked_files < file_by_compaction_pri.size() && !found_start_file) {
    int index = file_by_compaction_pri[compaction_pointer_index];
    
    if (index >= static_cast<int>(level_files.size())) {
      ROCKS_LOG_INFO(nullptr, "索引 %d 超出了文件数组范围 %zu，跳过", 
                     index, level_files.size());
      compaction_pointer_index = (compaction_pointer_index + 1) % file_by_compaction_pri.size();
      checked_files++;
      continue;
    }
    
    auto* file = level_files[index];
    
    if (file->being_compacted) {
      ROCKS_LOG_INFO(nullptr, "文件 %lu 已在compaction中，跳过", file->fd.GetNumber());
      compaction_pointer_index = (compaction_pointer_index + 1) % file_by_compaction_pri.size();
      checked_files++;
      continue;
    }
    
    // 找到了合适的起始文件
    start_file = file;
    start_index = index;
    found_start_file = true;
    
    ROCKS_LOG_INFO(nullptr, "找到起始文件: Level %d 文件 %lu (#%d), 键范围: [%s, %s], 大小: %zu bytes", 
                 level, file->fd.GetNumber(), index,
                 file->smallest.DebugString(true).c_str(),
                 file->largest.DebugString(true).c_str(),
                 static_cast<size_t>(file->fd.GetFileSize()));
    
    // 添加起始文件到预测集合
    std::string file_number = std::to_string(file->fd.GetNumber());
    files.insert(file_number);
    
    compaction_pointer_index = (compaction_pointer_index + 1) % file_by_compaction_pri.size();
    checked_files++;
  }
  
  // 如果没有找到起始文件，尝试其他策略
  if (!found_start_file) {
    ROCKS_LOG_INFO(nullptr, "从NextCompactionIndex开始未找到合适的起始文件，尝试选择最大文件");
    
    // 尝试选择大小最大的文件
    FileMetaData* largest_file = nullptr;
    uint64_t largest_size = 0;
    for (const auto& f : level_files) {
      if (f->being_compacted) continue;
      
      if (f->fd.GetFileSize() > largest_size) {
        largest_size = f->fd.GetFileSize();
        largest_file = f;
      }
    }
    
    if (largest_file != nullptr) {
      start_file = largest_file;
      std::string file_number = std::to_string(largest_file->fd.GetNumber());
      files.insert(file_number);
      
      ROCKS_LOG_INFO(nullptr, "基于文件大小选择: Level %d 文件 %s (大小: %llu bytes)", 
                   level, file_number.c_str(), 
                   static_cast<unsigned long long>(largest_size));
    } else {
      ROCKS_LOG_INFO(nullptr, "Level %d 没有可用于compaction的文件", level);
      return files;
    }
  }
  
  // 如果找到了起始文件，模拟ExpandInputsToCleanCut操作，寻找所有需要合并的文件
  if (start_file != nullptr) {
    // 确保所有与起始文件重叠的文件都被包含在预测中
    Slice smallest_key = start_file->smallest.user_key();
    Slice largest_key = start_file->largest.user_key();
    
    std::set<size_t> included_indices;
    if (start_index >= 0) {
      included_indices.insert(start_index);
    }
    
    // 扩展键范围，直到找不到更多的重叠文件
    size_t old_size;
    bool expanded = true;
    int expansion_rounds = 0;
    
    while (expanded) {
      old_size = included_indices.size();
      
      for (size_t i = 0; i < level_files.size(); i++) {
        if (included_indices.find(i) != included_indices.end()) {
          continue;  // 已经包含了该文件
        }
        
        const FileMetaData* other_file = level_files[i];
        if (other_file->being_compacted) {
          continue;  // 跳过正在compaction的文件
        }
        
        // 检查是否与当前键范围重叠
        Slice other_smallest = other_file->smallest.user_key();
        Slice other_largest = other_file->largest.user_key();
        
        bool overlaps = !(other_largest.compare(smallest_key) < 0 || 
                          other_smallest.compare(largest_key) > 0);
        
        if (overlaps) {
          std::string other_file_number = std::to_string(other_file->fd.GetNumber());
          files.insert(other_file_number);
          included_indices.insert(i);
          
          // 更新键范围
          if (other_smallest.compare(smallest_key) < 0) {
            smallest_key = other_smallest;
          }
          if (other_largest.compare(largest_key) > 0) {
            largest_key = other_largest;
          }
          
          ROCKS_LOG_INFO(nullptr, "找到重叠文件: Level %d 文件 %s 与起始文件键范围重叠，扩展键范围至 [%s, %s]", 
                        level, other_file_number.c_str(),
                        smallest_key.ToString(true).c_str(),
                        largest_key.ToString(true).c_str());
        }
      }
      
      expanded = (included_indices.size() > old_size);
      if (expanded) {
        expansion_rounds++;
        ROCKS_LOG_INFO(nullptr, "键范围扩展第 %d 轮，当前包含 %zu 个文件", 
                      expansion_rounds, included_indices.size());
      }
    }
  }
  
  ROCKS_LOG_INFO(nullptr, "Level %d 总共预测到 %zu 个文件将参与compaction", 
                level, files.size());
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

// 检查是否可能发生L1到L2的compaction，尽管L1的score < 1.0
bool CompactionPredictor::CheckL1ToL2Compaction() {
  // RocksDB中即使score < 1.0，也可能因为特殊情况进行compaction
  // 特别是当L0有积压，但L0->L1 compaction可能会导致L1过大时
  
  // 检查L0积压情况
  double l0_score = vstorage_->CompactionScore(0);
  if (l0_score <= 1.0) {
    return false;  // L0没有积压
  }
  
  // 检查L1的情况
  double l1_score = vstorage_->CompactionScore(1);
  if (l1_score >= 1.0) {
    return false;  // L1已经需要compaction了，不需要特殊处理
  }
  
  // 检查L1和L2的大小比例
  uint64_t l1_size = vstorage_->NumLevelBytes(1);
  uint64_t l2_size = vstorage_->NumLevelBytes(2);
  
  // 如果L1已经比较大，接近其目标大小的70%以上
  if (l1_score >= 0.7) {
    ROCKS_LOG_INFO(nullptr, "L1 score (%.2f) 接近1.0且L0有积压(score: %.2f)，可能发生L1->L2 compaction",
                  l1_score, l0_score);
    return true;
  }
  
  // 如果L1文件比较多
  if (vstorage_->NumLevelFiles(1) >= 8) {
    ROCKS_LOG_INFO(nullptr, "L1有较多文件(%d)且L0有积压(score: %.2f)，可能发生L1->L2 compaction",
                  vstorage_->NumLevelFiles(1), l0_score);
    return true;
  }
  
  // 如果L1远大于L2，也可能触发compaction
  if (l2_size > 0 && l1_size > 2 * l2_size) {
    ROCKS_LOG_INFO(nullptr, "L1大小(%llu)远大于L2(%llu)，可能发生L1->L2 compaction",
                 static_cast<unsigned long long>(l1_size), 
                 static_cast<unsigned long long>(l2_size));
    return true;
  }
  
  return false;
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
  
  // 寻找未被排除的文件作为新的起点
  const FileMetaData* start_file = nullptr;
  for (const auto& file : level_files) {
    std::string file_number = std::to_string(file->fd.GetNumber());
    if (excluded_files.find(file_number) == excluded_files.end()) {
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
    if (excluded_files.find(file_number) != excluded_files.end()) {
      continue;  // 已被排除，跳过
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
      if (excluded_files.find(file_number) != excluded_files.end()) {
        continue;  // 已被排除，跳过
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
  
  // 简化处理：不需要添加到incorrect_predicted_files_集合中，只需删除即可
  // incorrect_predicted_files_.insert(incorrect_files.begin(), incorrect_files.end());
}

} // namespace ROCKSDB_NAMESPACE