using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;

class SharableSpreadSheet
{
    private string[,] data;
    private int rows, cols;
    private int userLimit;

    // Global lock for structural changes (row/col add, save/load)
    // - Writers: structure-changing ops
    // - Readers: cell operations (get/set/search)
    private ReaderWriterLockSlim globalLock = new ReaderWriterLockSlim(); //global lock for sheet changes, for example, adding a row/column (write), or saving(read) 

    // Partition locks for fine-grained concurrency on cells
    private ReaderWriterLockSlim[] userLocks; //partitioned lockes responsible for certain parts of the spreadsheet, they are split 

    public SharableSpreadSheet(int nRows, int nCols, int nUsers = -1) //initialising the shareablespreadhseet class.
    {
        rows = nRows;
        cols = nCols;
        userLimit = nUsers;
        data = new string[rows, cols];

        InitializeLocks(); //initialising the locks using separate function
    }


    private void InitializeLocks()    //initializing partitioned locks (2nd layer) based on current size and userimit
    {
        int cellCount = rows * cols;
        int lockCount = Math.Max(1, (userLimit > 0) ? Math.Min(userLimit, cellCount) : Math.Min(Environment.ProcessorCount * 2, cellCount));         //calculating number of locks: At least 1, capped by userLimit and cell count for balance
        userLocks = new ReaderWriterLockSlim[lockCount]; //using a new number of locks, and initialising all to a readerwriter lock
        for (int i = 0; i < lockCount; i++)
            userLocks[i] = new ReaderWriterLockSlim();
    }


    private void ResizeLocksIfNeeded()  // recalculating partition locks to adapt to new size, called under the globallock only
    {
        int cellCount = rows * cols;
        int desiredLockCount = Math.Max(1, (userLimit > 0) ? Math.Min(userLimit, cellCount) : Math.Min(Environment.ProcessorCount * 2, cellCount)); //potential resizing

        if (desiredLockCount != userLocks.Length) //resising only if needed
        {
            
            foreach (var l in userLocks)
                l.Dispose(); //disposing old locks

            userLocks = new ReaderWriterLockSlim[desiredLockCount];
            for (int i = 0; i < desiredLockCount; i++)
                userLocks[i] = new ReaderWriterLockSlim();
        }
    }

    
    private ReaderWriterLockSlim GetLockForCell(int row, int col) //mapping a cell to a partition lock index
    {
        int index = ((row * cols + col) % userLocks.Length); //using a simple function for mapping a the place of a cell to a lock
        return userLocks[index]; //returning the readerwriter lock
    }

    private void ValidateIndices(int row, int col) //simple function to check that inserted place is valid
    {
        ValidateCol(col);
        ValidateRow(row);
    }

    private void ValidateRow(int row) //same here, just for rows
    {
        if (row < 0 || row >= rows)
            throw new ArgumentOutOfRangeException($"Invalid row: {row}");
    }

    private void ValidateCol(int col) //same here, just for columns
    {
        if (col < 0 || col >= cols)
            throw new ArgumentOutOfRangeException($"Invalid column: {col}");
    }


    public string getCell(int row, int col) //getting the text of a cell, needs globalLock read and cell lock read
    {
        ValidateIndices(row, col); //checking if indicies are fine

   
        globalLock.EnterReadLock(); //preventing writer functions
        try
        {
            var cellLock = GetLockForCell(row, col); //getting the partitioned local lock
            cellLock.EnterReadLock();
            try
            {
                return data[row, col]; //returning the text
            }
            finally
            {
                cellLock.ExitReadLock(); //exiting global and local locks
            }
        }
        finally
        {
            globalLock.ExitReadLock();
        }
    }


    public void setCell(int row, int col, string str)    //editing cell operation which needs globalLock read and cell lock write
    {
        ValidateIndices(row, col); //validating

        globalLock.EnterReadLock(); //since this is a local function, it has global read accessibility
        try
        {
            var cellLock = GetLockForCell(row, col);
            cellLock.EnterWriteLock(); //since its a local writer function, it has local write accessibility
            try
            {
                data[row, col] = str; //chaning the value
            }
            finally
            { 
                cellLock.ExitWriteLock(); //exiting local write lock
            }
        }
        finally
        {
            globalLock.ExitReadLock(); //exiting global read lock
        }
    }


    public void addRow(int row1) //adding a row to the spreadsheet,this is a global write function
    {
        if (row1 < -1 || row1 >= rows) throw new ArgumentOutOfRangeException(); //making sure the indexes are fine, -1 means adding it to the left of 0

        // Acquire exclusive global lock - prevents cell reads/writes during resize
        globalLock.EnterWriteLock(); //accessing the global write lock
        try
        {
        
            string[,] newData = new string[rows + 1, cols]; //creating new data array with one more row

            for (int r = 0; r <= row1; r++) //all rows up to row1 stay the exact same so we just copy them
                for (int c = 0; c < cols; c++)
                    newData[r, c] = data[r, c];


            for (int r = row1 + 1; r < rows + 1; r++)            //inserting an empty row after row1, shifting remaining rows 1 to the right
                for (int c = 0; c < cols; c++)
                    newData[r, c] = (r == row1 + 1) ? "" : data[r - 1, c];

            
            data = newData; //replacing the old data and increasing the number of rows
            rows++;

            ResizeLocksIfNeeded(); //adding another lock if needed
        }
        finally
        {
            globalLock.ExitWriteLock(); //releasing the global write lock
        }
    }

    public void addCol(int col1) //very similar to addRow, just for col
    {
        if (col1 < -1 || col1 >= cols) throw new ArgumentOutOfRangeException();

        globalLock.EnterWriteLock();
        try
        {
            string[,] newData = new string[rows, cols + 1];

            for (int r = 0; r < rows; r++)
            {
                for (int c = 0; c <= col1; c++)
                    newData[r, c] = data[r, c];
                newData[r, col1 + 1] = "";
                for (int c = col1 + 1; c < cols; c++)
                    newData[r, c + 1] = data[r, c];
            }

            data = newData;
            cols++;

            ResizeLocksIfNeeded();
        }
        finally
        {
            globalLock.ExitWriteLock();
        }
    }

    // Row exchange requires multiple cell locks write lock, global lock in read mode to prevent structural changes
    public void exchangeRows(int row1, int row2)
    {
        ValidateRow(row1);
        ValidateRow(row2);

        globalLock.EnterReadLock();
        try
        {
            for (int c = 0; c < cols; c++)
            {
                int i1 = row1 * cols + c;
                int i2 = row2 * cols + c;

                // Enforce consistent lock order to avoid deadlock
                if (i1 > i2)
                {
                    (i1, i2) = (i2, i1);
                    (row1, row2) = (row2, row1);
                }

                var lock1 = GetLockForCell(row1, c);
                var lock2 = GetLockForCell(row2, c);

                lock1.EnterWriteLock();
                lock2.EnterWriteLock();
                try
                {
                    string temp = data[row1, c];
                    data[row1, c] = data[row2, c];
                    data[row2, c] = temp;
                }
                finally
                {
                    lock2.ExitWriteLock();
                    lock1.ExitWriteLock();
                }
            }
        }
        finally
        {
            globalLock.ExitReadLock();
        }
    }

    // Column exchange works similarly to row exchange
    public void exchangeCols(int col1, int col2)
    {
        ValidateCol(col1);
        ValidateCol(col2);

        globalLock.EnterReadLock();
        try
        {
            for (int r = 0; r < rows; r++)
            {
                int i1 = r * cols + col1;
                int i2 = r * cols + col2;

                if (i1 > i2)
                {
                    (i1, i2) = (i2, i1);
                    (col1, col2) = (col2, col1);
                }

                var lock1 = GetLockForCell(r, col1);
                var lock2 = GetLockForCell(r, col2);

                lock1.EnterWriteLock();
                lock2.EnterWriteLock();
                try
                {
                    string temp = data[r, col1];
                    data[r, col1] = data[r, col2];
                    data[r, col2] = temp;
                }
                finally
                {
                    lock2.ExitWriteLock();
                    lock1.ExitWriteLock();
                }
            }
        }
        finally
        {
            globalLock.ExitReadLock();
        }
    }

    // Search operations only require read access on both layers
    public Tuple<int, int> searchString(string str)
    {
        globalLock.EnterReadLock();
        try
        {
            for (int r = 0; r < rows; r++)
            {
                for (int c = 0; c < cols; c++)
                {
                    var cellLock = GetLockForCell(r, c);
                    cellLock.EnterReadLock();
                    try
                    {
                        if (data[r, c] == str)
                            return Tuple.Create(r, c);
                    }
                    finally
                    {
                        cellLock.ExitReadLock();
                    }
                }
            }
        }
        finally
        {
            globalLock.ExitReadLock();
        }
        return null;
    }

    // Save operation acquires global write lock (blocks structure and cell ops)
    public void save(string filename)
    {
        globalLock.EnterWriteLock();
        try
        {
            using (StreamWriter sw = new StreamWriter(filename))
            {
                for (int r = 0; r < rows; r++)
                {
                    List<string> rowValues = new List<string>();
                    for (int c = 0; c < cols; c++)
                        rowValues.Add(data[r, c] ?? "");
                    sw.WriteLine(string.Join(",", rowValues));
                }
            }
        }
        finally
        {
            globalLock.ExitWriteLock();
        }
    }

    // Load operation acquires global write lock
    public void load(string filename)
    {
        globalLock.EnterWriteLock();
        try
        {
            var lines = File.ReadAllLines(filename);
            int newRows = lines.Length;
            int newCols = 0;

            if (newRows > 0)
                newCols = lines[0].Split(',').Length;

            var newData = new string[newRows, newCols];
            for (int r = 0; r < newRows; r++)
            {
                var parts = lines[r].Split(',');
                for (int c = 0; c < newCols; c++)
                {
                    newData[r, c] = (c < parts.Length) ? parts[c] : "";
                }
            }

            data = newData;
            rows = newRows;
            cols = newCols;

            ResizeLocksIfNeeded();
        }
        finally
        {
            globalLock.ExitWriteLock();
        }
    }
}
