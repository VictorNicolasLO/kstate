import {createHash} from 'node:crypto'
import {ulid} from 'ulid'
function getPartition(stringId, N) {
    // Create an MD5 hash of the stringId
    const hash = createHash('md5').update(stringId).digest('hex');
    // Convert the hex hash to an integer
    const intHash = BigInt('0x' + hash);
    // Get the partition number
    const partition = Number(intHash % BigInt(N));
    return partition;
  }
  
  for(let i = 0; i < 100; i++) {
    const id = ulid()
    const partition = getPartition(id, 10)
    console.log(id, partition)
  }
