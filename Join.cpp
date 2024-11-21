#include "Join.hpp"
#include <vector>

using namespace std;

/*
 * Input: Disk, Memory, Disk page ids for left relation, Disk page ids for right relation
 * Output: Vector of Buckets of size (MEM_SIZE_IN_PAGE - 1) after partition
 */
vector<Bucket> partition(Disk* disk, Mem* mem, pair<uint, uint> left_rel,
                         pair<uint, uint> right_rel) {
	// TODO: implement partition phase
	//vector<Bucket> partitions(0, Bucket(disk)); // placeholder
	//return partitions;
    // Create B-1 buckets (each bucket has its own buffer page internally)
    vector<Bucket> partitions(MEM_SIZE_IN_PAGE - 1, Bucket(disk));

     mem->reset();
    // Process the left relation (R)
    for (uint page_id = left_rel.first; page_id < left_rel.second; ++page_id) {
        mem->loadFromDisk(disk, page_id, MEM_SIZE_IN_PAGE - 1); // Load disk page to memory page 0
        Page* page = mem->mem_page(MEM_SIZE_IN_PAGE - 1);

        for (uint i = 0; i < page->size(); ++i) {
            Record record = page->get_record(i);

            // Determine which bucket this record belongs to
            uint bucket_id = record.partition_hash() % (MEM_SIZE_IN_PAGE - 1);

            // Add record to the bucket
           partitions[bucket_id].add_left_rel_page(page_id);

            // Check if the bucket needs to flush (if it's full)
            if (partitions[bucket_id].num_left_rel_record >= RECORDS_PER_PAGE) {
                uint flushed_page_id = mem->flushToDisk(disk, bucket_id + 1);
                partitions[bucket_id].add_left_rel_page(flushed_page_id);
            }
        }
    }

    // Flush any remaining records in partially filled buckets for R
    for (uint i = 0; i < partitions.size(); ++i) {
        if (partitions[i].num_left_rel_record > 0) {
            uint flushed_page_id = mem->flushToDisk(disk, i + 1);
            partitions[i].add_left_rel_page(flushed_page_id);
        }
    }
    mem->reset();
    // Process the right relation (S)
    for (uint page_id = right_rel.first; page_id < right_rel.second; ++page_id) {
        mem->loadFromDisk(disk, page_id, MEM_SIZE_IN_PAGE - 1); // Load disk page to memory page 0
        Page* page = mem->mem_page(MEM_SIZE_IN_PAGE - 1);

        for (uint i = 0; i < page->size(); ++i) {
            Record record = page->get_record(i);

            // Determine which bucket this record belongs to
            uint bucket_id = record.partition_hash() % (MEM_SIZE_IN_PAGE - 1);

            // Add record to the bucket
            partitions[bucket_id].add_right_rel_page(page_id);

            // Check if the bucket needs to flush (if it's full)
            if (partitions[bucket_id].num_right_rel_record >= RECORDS_PER_PAGE) {
                uint flushed_page_id = mem->flushToDisk(disk, bucket_id + 1);
                partitions[bucket_id].add_right_rel_page(flushed_page_id);
            }
        }
    }

    // Flush any remaining records in partially filled buckets for S
    for (uint i = 0; i < partitions.size(); ++i) {
        if (partitions[i].num_right_rel_record > 0) {
            uint flushed_page_id = mem->flushToDisk(disk, i + 1);
            partitions[i].add_right_rel_page(flushed_page_id);
        }
    }

    return partitions;
}

/*
 * Input: Disk, Memory, Vector of Buckets after partition
 * Output: Vector of disk page ids for join result
 */
vector<uint> probe(Disk* disk, Mem* mem, vector<Bucket>& partitions) {
	// TODO: implement probe phase
	//vector<uint> disk_pages; // placeholder
	//return disk_pages;
    vector<uint> disk_pages;
     mem->reset();

    for (Bucket& bucket : partitions) {

        vector<uint> big_rel;
        vector<uint> small_rel;

        //check which one is outer
        if (bucket.num_left_rel_record > bucket.num_right_rel_record) {
            big_rel = bucket.get_left_rel();
            small_rel = bucket.get_right_rel();
        }
        else {
            big_rel = bucket.get_right_rel();
            small_rel = bucket.get_left_rel();
        }

        Page* page = mem->mem_page(MEM_SIZE_IN_PAGE - 1);
        Record small("","");
        uint small_hash = 0;

        // Load small relation records into memory
        for (uint left_page_id : small_rel) {
            mem->loadFromDisk(disk, left_page_id, MEM_SIZE_IN_PAGE - 1);
            small = page->get_record(left_page_id);
            //hash all small relations
            small_hash = small.probe_hash() % (MEM_SIZE_IN_PAGE - 2);
        }

        Record big("","");
        uint big_hash = 0;

        for (uint right_page_id : big_rel) {
            mem->loadFromDisk(disk, right_page_id, MEM_SIZE_IN_PAGE - 1);
            big = page->get_record(right_page_id);
            //hash all big
            big_hash = big.probe_hash() % (MEM_SIZE_IN_PAGE - 2);
        }

        for (uint j = 0; j < mem->mem_page(MEM_SIZE_IN_PAGE - 1)->size(); ++j) {
            // Check if hashes match and keys match
            if (small_hash == big_hash && small == big) {
                Page* output_page = mem->mem_page(MEM_SIZE_IN_PAGE - 1);
                if (output_page->full()) {
                    uint page_id = mem->flushToDisk(disk, MEM_SIZE_IN_PAGE - 1);
                    disk_pages.push_back(page_id);
                }
                output_page->loadPair(big, small);
            }
        }


        // Perform join with big relation
        // for (uint right_page_id : big_rel) {
        //     mem->loadFromDisk(disk, right_page_id, 1);
        //     Page* right_page = mem->mem_page(MEM_SIZE_IN_PAGE - 2);

        //     for (uint i = 0; i < right_page->size(); ++i) {
        //         Record right_record = right_page->get_record(i);
        //         uint right_hash = right_record.probe_hash() % (MEM_SIZE_IN_PAGE - 2);

        //         for (uint j = 0; j < mem->mem_page(0)->size(); ++j) {
        //             Record left_record = mem->mem_page(0)->get_record(j);
        //             uint left_hash = left_record.probe_hash() % (MEM_SIZE_IN_PAGE - 2) ;

        //             // Check if hashes match and keys match
        //             if (left_hash == right_hash && left_record == right_record) {
        //                 Page* output_page = mem->mem_page(2);
        //                 if (output_page->full()) {
        //                     uint page_id = mem->flushToDisk(disk, 2);
        //                     disk_pages.push_back(page_id);
        //                 }
        //                 output_page->loadPair(left_record, right_record);
        //             }
        //         }
        //     }
        // }
    }
	
	// Flush remaining output page
	if (!mem->mem_page(2)->empty()) {
		uint page_id = mem->flushToDisk(disk, 2);
		disk_pages.push_back(page_id);
	}


    return disk_pages;
}