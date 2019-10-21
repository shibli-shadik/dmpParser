package dmpparser;

public enum FileStatus 
{
    NEW("New"),
    PROCESSING("Processing"),        
    PROCESSED("Processed"),
    REJECTED("Rejected");
    
    private final String displayName;
    
    FileStatus(String displayName) 
    {
        this.displayName = displayName;
    }
    
    public String getDisplayName() 
    {
        return displayName;
    }
    
    public static FileStatus getEnumByString(String code)
    {
        for(FileStatus item : FileStatus.values())
        {
            if(code.equals(item.displayName)) 
            {
                return item;
            }
        }
        
        return null;
    }
}
