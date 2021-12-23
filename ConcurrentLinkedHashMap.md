ConcurrentLinkedHashMap

### 基本实现
由一个并发 ConcurrentMap，以及一个链表 LinkedDeque 组成。  
读写时先同步修改 data(ConcurrentMap)，然后再异步/同步修改 evictionDeque(LinkedDeque)。
写作业 task(Runnable) 直接先入队列，等读操作积压到阈值时，顺带异步处理 16 个写操作，后台会新建 Runnable 线程处理掉，这里处理指操作 evictionDeque 等。  
读操作不用新建线程，而是积压，到一定数量后，一次性修改多次 evictionDeque。

一个元素有三种状态：  
- Alive, weight > 0, 即元素在 hash-table 且在队列，没有被替换掉。
- Retired, weight < 0, 即元素不在 hash-table 且在队列，等待被替换掉。
- Dead, weight == 0, 即元素既不在 hash-table 也不在队列，已经被替换掉。

### 接口
~~~cpp
V get(object key);                          // 普通 get
V getQuietly(Object key);                   // 只操作 hash-table，不会引起替换策略
V put(K key, V value);                      // 存在同样的key时，可替换
V putIfAbsent(K key, V value);              // 存在同样的key时，不替换，返回旧值
V remove(Object key);
boolean remove(Object key, Object value);
V replace(K key, V value);
boolean replace(K key, V oldValue, V newValue);
~~~

### 读操作
ConcurrentLinkedHashMap 将读操作按调用者的线程 id 区分开，开辟了一个二维数组 readBuffers[NUMBER_OF_READ_BUFFERS][READ_BUFFER_SIZE] 来存放读操作。  
NUMBER_OF_READ_BUFFERS = 32，为逻辑核数；  
READ_BUFFER_SIZE = 128(32*4)。

每个线程产生的读操作会被分配到 readBuffers[thd_id % 32] 中，  
具体被分配到那个 slot 呢？（readBuffers[thd_id % 32][?]）  
作者为所有进入数组的读操作设置了32个计数器，且同时为处理完的操作也设置了32个计数器，分别是：  
- readBufferReadCount[32]
- readBufferWriteCount[32]
每个计数器都是原子操作，且只增不减。
readBufferDrainAtWriteCount[32] 用于记录已经处理掉的读操作数。  

Pseudo code：
~~~cpp
read_thd -> get() {
    bufferIndex = thd_id % 32
    long pending = (readBufferWriteCount[bufferIndex] - readBufferDrainAtWriteCount[bufferIndex].get());
    if(pending >= 32) {
        // 先处理当前线程的积压，然后顺便把其他线程的也处理了
        drainReadBuffers() {
            final int start = (int) Thread.currentThread().getId();
            final int end = start + NUMBER_OF_READ_BUFFERS;
            for (int i = start; i < end; i++) {
                drainReadBuffer(i & READ_BUFFERS_MASK) {
                    final long writeCount = readBufferWriteCount[bufferIndex].get();
                    // 虽然一个线程最多积压128个，但是每次只处理 64 个
                    for (int i = 0; i < 64; i++) {
                        // 不是从 0 开始，而是从上次最后一个位置开始
                        final int index = (int) (readBufferReadCount[bufferIndex] & READ_BUFFER_INDEX_MASK);
                        final AtomicReference<Node<K, V>> slot = readBuffers[bufferIndex][index];
                        final Node<K, V> node = slot.get();
                        if (node == null) {
                            break;
                        }
                        slot.lazySet(null);
                        applyRead(node);
                        readBufferReadCount[bufferIndex]++;
                    }
                    // 处理完修改已经处理的个数
                    readBufferDrainAtWriteCount[bufferIndex].lazySet(writeCount);
                }
            }
        }
    }
}
~~~

### update
在 put 时，会遇到旧值，如果旧值存在，则先用原子操作替换掉 hash-table 的元素，但是元素的 weight 是通过异步 afterWrite 修改的，传给 update 的是一个旧值node和一个weight差值。


### evict
判断当前是否满，一直处理到有空间为止。  
先 node = evictionDeque.poll(); 再 map.delete(node.key, value); 此时 value 结构体还没有析构，作者将其加入一个队列并 makeDead，后面适当的时候会同步删除这个结构。

### remove
该操先删除 map 中的元素，从 queue 中删除是异步的。如果同时对同一个元素 put 和 remove，这两个异步操作顺序不定，需要注意。


### 元素权重 weight
