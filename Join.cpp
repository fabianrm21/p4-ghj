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

	vector<Bucket> partitions(MEM_SIZE_IN_PAGE - 1, Bucket(disk));

    // Process the left relation
    for (uint page_id = left_rel.first; page_id < left_rel.second; ++page_id) {
        mem->loadFromDisk(disk, page_id, 0); // Load disk page to memory page 0
        Page* page = mem->mem_page(0);
        for (uint i = 0; i < page->size(); ++i) {
            Record record = page->get_record(i);
            uint bucket_id = record.partition_hash() % (MEM_SIZE_IN_PAGE - 1);
            partitions[bucket_id].add_left_rel_page(page_id);
        }
		mem->reset();
    }
	// Flush remaining pages for left relation
    for (uint bucket_id = 0; bucket_id < partitions.size(); ++bucket_id) {
        Page* output_page = mem->mem_page(bucket_id + 1);
        if (!output_page->empty()) {
            uint page_id = mem->flushToDisk(disk, bucket_id + 1);
            partitions[bucket_id].add_left_rel_page(page_id);
            output_page->reset();
        }
    }


    // Process the right relation
    for (uint page_id = right_rel.first; page_id < right_rel.second; ++page_id) {
        mem->loadFromDisk(disk, page_id, 0); // Load disk page to memory page 0
        Page* page = mem->mem_page(0);
        for (uint i = 0; i < page->size(); ++i) {
            Record record = page->get_record(i);
            uint bucket_id = record.partition_hash() % (MEM_SIZE_IN_PAGE - 1);
            partitions[bucket_id].add_right_rel_page(page_id);
        }
		mem->reset();
    }
	// Flush remaining pages for right relation
    for (uint bucket_id = 0; bucket_id < partitions.size(); ++bucket_id) {
        Page* output_page = mem->mem_page(bucket_id + 1);
        if (!output_page->empty()) {
            uint page_id = mem->flushToDisk(disk, bucket_id + 1);
            partitions[bucket_id].add_right_rel_page(page_id);
            output_page->reset();
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

    for (Bucket& bucket : partitions) {
        vector<uint> left_pages = bucket.get_left_rel();
        vector<uint> right_pages = bucket.get_right_rel();

        // Clear memory for each bucket processing
        mem->reset();

        // Load left relation records into memory
        for (uint left_page_id : left_pages) {
            mem->loadFromDisk(disk, left_page_id, 0);
        }

        // Perform join with right relation
        for (uint right_page_id : right_pages) {
            mem->loadFromDisk(disk, right_page_id, 1);
            Page* right_page = mem->mem_page(1);

            for (uint i = 0; i < right_page->size(); ++i) {
                Record right_record = right_page->get_record(i);
                uint right_hash = right_record.probe_hash();

                for (uint j = 0; j < mem->mem_page(0)->size(); ++j) {
                    Record left_record = mem->mem_page(0)->get_record(j);
                    uint left_hash = left_record.probe_hash();

                    // Check if hashes match and keys match
                    if (left_hash == right_hash && left_record == right_record) {
                        Page* output_page = mem->mem_page(2);
                        if (output_page->full()) {
                            uint page_id = mem->flushToDisk(disk, 2);
                            disk_pages.push_back(page_id);
                        }
                        output_page->loadPair(left_record, right_record);
                    }
                }
            }
        }
		// Flush remaining output page
		if (mem->mem_page(2)->full()) {
			uint page_id = mem->flushToDisk(disk, 2);
			disk_pages.push_back(page_id);
		}
    }
	// Flush remaining output page
	// flush todo lo que quede, no solo este
	if (!mem->mem_page(2)->empty()) {
		uint page_id = mem->flushToDisk(disk, 2);
		disk_pages.push_back(page_id);
	}
    return disk_pages;
}