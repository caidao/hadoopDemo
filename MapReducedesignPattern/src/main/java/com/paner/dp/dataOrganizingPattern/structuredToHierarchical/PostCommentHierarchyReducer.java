package com.paner.dp.dataOrganizingPattern.structuredToHierarchical;

import com.paner.utils.CommonUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.dom4j.Document;
import org.dom4j.DocumentFactory;
import org.dom4j.Element;

import javax.xml.parsers.DocumentBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @User: paner
 * @Date: 17/10/28 下午11:28
 */
public class PostCommentHierarchyReducer extends Reducer<Text,Text,Text,NullWritable> {

    private ArrayList<String> comments = new ArrayList<String>();
    private Document document = null;
    private String post = null;

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        post = null;

        comments.clear();

        for (Text t:values){
            if (t.charAt(0) == 'P'){
                post = t.toString().substring(1,t.toString().length()).trim();
            }else {
                comments.add(t.toString().substring(1,t.toString().length()).trim());
            }
        }

        if (post!=null){
            String postWithCommentChildren = nestElements(post,comments);
            context.write(new Text(postWithCommentChildren),NullWritable.get());
        }
    }

    private String nestElements(String post,List<String> comments){

        document = DocumentFactory.getInstance().createDocument("utf-8");
        Element postE1 = CommonUtil.getXmlElemetFromString(post);
        Element toAddPostE1 = document.addElement("post");

        CommonUtil.copyAttrubutesToElement(postE1.attributes(),toAddPostE1);

        for (String comment:comments){
            Element commentE1 = CommonUtil.getXmlElemetFromString(comment);
            Element toAddCommentE1 = document.addElement("comments");
            CommonUtil.copyAttrubutesToElement(commentE1.attributes(),toAddCommentE1);

            toAddPostE1.add(commentE1);
        }

        return document.asXML();
    }
}
