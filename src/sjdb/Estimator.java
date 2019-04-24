package sjdb;

import org.w3c.dom.Attr;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

public class Estimator implements PlanVisitor {


	public Estimator() {
		// empty constructor
	}

	/* 
	 * Create output relation on Scan operator
	 *
	 * Example implementation of visit method for Scan operators.
	 */
	public void visit(Scan op) {
		Relation input = op.getRelation();
		Relation output = new Relation(input.getTupleCount());
		
		Iterator<Attribute> iter = input.getAttributes().iterator();
		while (iter.hasNext()) {
			output.addAttribute(new Attribute(iter.next()));
		}
		
		op.setOutput(output);
	}

	public void visit(Project op) {
		Relation output = new Relation(op.getInput().getOutput().getTupleCount());
		Relation input = op.getInput().getOutput();

		Attribute now;
		Iterator<Attribute> it = input.getAttributes().iterator();
		while (it.hasNext()) {
			now = it.next();
			if(op.getAttributes().contains(now))
				output.addAttribute(new Attribute(now));
		}

		op.setOutput(output);
	}
	
	public void visit(Select op) {
		Relation input = op.getInput().getOutput();
		Relation output;

		if(op.getPredicate().equalsValue()) {
			output = new Relation(input.getTupleCount()/input.getAttribute(op.getPredicate().getLeftAttribute()).getValueCount());
			Iterator<Attribute> it = input.getAttributes().iterator();
			Attribute now;
			Attribute left_pred= op.getPredicate().getLeftAttribute();
			while (it.hasNext()) {
				now = it.next();
				if(now.equals(left_pred)){
					output.addAttribute(new Attribute(now.getName(),1));
				}else {
					output.addAttribute(new Attribute(now));
				}
			}
		}else{
			Attribute left = input.getAttribute(op.getPredicate().getLeftAttribute());
			Attribute right = input.getAttribute(op.getPredicate().getRightAttribute());

			output = new Relation(input.getTupleCount()/Math.max(left.getValueCount(),right.getValueCount()));

			Iterator<Attribute> it = input.getAttributes().iterator();
			Attribute now;
			while (it.hasNext()) {
				now = it.next();
				if (now.equals(left) || now.equals(right)) {
					output.addAttribute(new Attribute(now.getName(), Math.min(left.getValueCount(), right.getValueCount())));
				} else {
					output.addAttribute(new Attribute(now));
				}
			}
		}
		op.setOutput(output);

	}
	
	public void visit(Product op) {
		Relation left = op.getLeft().getOutput();
		Relation right = op.getRight().getOutput();
		Relation output = new Relation(left.getTupleCount()*right.getTupleCount());

		Iterator<Attribute> iter_l = left.getAttributes().iterator();
		while (iter_l.hasNext()) {
			output.addAttribute(new Attribute(iter_l.next()));
		}

		Iterator<Attribute> iter_r = right.getAttributes().iterator();
		while (iter_r.hasNext()) {
			output.addAttribute(new Attribute(iter_r.next()));
		}

		op.setOutput(output);
	}
	
	public void visit(Join op) {
		Relation left = op.getLeft().getOutput();
		Relation right = op.getRight().getOutput();

		Attribute left_pred = op.getPredicate().getLeftAttribute();
		Attribute right_pred = op.getPredicate().getRightAttribute();

		Relation output = new Relation(left.getTupleCount() * right.getTupleCount() /Math.max(left_pred.getValueCount(),right_pred.getValueCount()));
		Attribute now;

		Iterator<Attribute> left_it = left.getAttributes().iterator();
		while (left_it.hasNext()){
			now = left_it.next();
			if(now.equals(left_pred)){
				output.addAttribute(new Attribute(now.getName(),Math.min(left_pred.getValueCount(),right_pred.getValueCount())));
			}else {
				output.addAttribute(new Attribute(now.getName(),now.getValueCount()));
			}
		}

		Iterator<Attribute> right_it = right.getAttributes().iterator();
		while (right_it.hasNext()){
			now =  right_it.next();
			if(now.equals(right_pred)) {
				output.addAttribute(new Attribute(now.getName(), Math.min(left_pred.getValueCount(), right_pred.getValueCount())));
			}
			else {
				output.addAttribute(new Attribute(now.getName(),now.getValueCount()));
			}
		}

		op.setOutput(output);


	}
}
