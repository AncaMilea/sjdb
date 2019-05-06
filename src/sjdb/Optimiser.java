package sjdb;

import java.util.*;

public class Optimiser {
    Catalogue catal;
    Estimator est;
    List<Operator> scans;
    PriorityQueue<Operator> selects_Value;
    PriorityQueue<Operator> selects_Att;
    PriorityQueue<Predicate> selects_Join;
    List<Select> left_selects;
    PriorityQueue<Join> joins;
    HashMap<Operator, Operator> select_scan_coupled;
    HashMap<Attribute,Scan> all_scans_att;
    List<Scan> used_scans ;
    List<Operator> op_for_product;
    List<Product> products;
    List<Attribute> all_attributes;
    List<Attribute> project_is_null_att;
    int ok;
    public Optimiser(Catalogue c){
        this.catal= c;
        est = new Estimator();
        scans = new ArrayList<>();
        selects_Value = new PriorityQueue<Operator>();
        selects_Att = new PriorityQueue<Operator>();
        selects_Join = new PriorityQueue<>((Predicate o1, Predicate o2) -> {
            return Math.min(o1.getLeftAttribute().getValueCount(),o2.getRightAttribute().getValueCount()) - Math.min(o2.getLeftAttribute().getValueCount(),o2.getRightAttribute().getValueCount());
        });
        joins = new PriorityQueue<>((Join j1, Join j2) ->{
            return Math.min(j1.getPredicate().getLeftAttribute().getValueCount(),j2.getPredicate().getRightAttribute().getValueCount()) - Math.min(j2.getPredicate().getLeftAttribute().getValueCount(),j1.getPredicate().getRightAttribute().getValueCount());
        });
        select_scan_coupled = new HashMap<>();
        all_scans_att = new HashMap<>();
        used_scans=new ArrayList<>();
        op_for_product=new ArrayList<>();
        products= new ArrayList<>();
        all_attributes = new ArrayList<>();
        project_is_null_att= new ArrayList<>();
        left_selects= new ArrayList<>();
        ok=0;
    }

    public Operator optimise(Operator pl)
    {
        schelet(pl);
        ScanCoupledWithSelect();

        JoinCreation();
        Operator op;
        op= ProductCreation();

        if(ok==0){
            for(Attribute sc:all_scans_att.keySet()) {
                project_is_null_att.add(sc);
            }
            List<Attribute> copy = new ArrayList<>(project_is_null_att);
            Project project = new Project(MakeProjections(op, project_is_null_att),copy);
            project.accept(est);
            return project;
        }else{
            List<Attribute> copy = new ArrayList<>(all_attributes);
            Project pj = new Project(MakeProjections(op, all_attributes),copy);
            pj.accept(est);
            return pj;
        }

    }
    public void schelet(Operator pl)
    {

        if(pl instanceof Select )
        {
            Select temp = (Select) pl;
            //System.out.println(temp.getPredicate());
            schelet(temp.getInput());
            Selecting(temp);

        }
        if(pl instanceof Project )
        {
            Project temp = (Project) pl;
            for(Attribute at : temp.getOutput().getAttributes())
            {
                System.out.println(at);
            }
            all_attributes.addAll(temp.getOutput().getAttributes());
            System.out.println(temp.getAttributes());
            schelet(temp.getInput());
            ok=1;

        }
        if(pl instanceof Scan )
        {
            Scan temp = (Scan) pl;
            Scanning(temp);
           // System.out.println(temp.toString());

        }
        if(pl instanceof Product )
        {
            Product temp = (Product) pl;
            schelet(temp.getLeft());
            schelet(temp.getRight());

        }

    }

    public void Scanning(Scan sc)
    {
        NamedRelation current = (NamedRelation) sc.getRelation();
        NamedRelation new_sc = new NamedRelation(current.toString(),current.getTupleCount());

        Iterator<Attribute> new_att= current.getAttributes().iterator();
        Attribute now;
        while (new_att.hasNext())
        {
            now= new_att.next();
            new_sc.addAttribute(new Attribute(now.getName(),now.getValueCount()));
        }

        Scan scan_op= new Scan(new_sc);
        Iterator<Attribute> att= ((Scan) scan_op).getRelation().getAttributes().iterator();
        Attribute new_at;
        while(att.hasNext()){
            new_at = att.next();
            all_scans_att.put(new_at,scan_op);
        }
        scan_op.accept(est);
        scans.add(scan_op);
    }

    public void Selecting(Select sl)
    {
        if(sl.getPredicate().getRightValue() == null)
        {
            Attribute left_old = sl.getPredicate().getLeftAttribute();
            Attribute right_old = sl.getPredicate().getRightAttribute();

            Attribute left_new = new Attribute(left_old.getName(), left_old.getValueCount());
            Attribute right_new = new Attribute(right_old.getName(), right_old.getValueCount());
            Predicate pred = new Predicate(left_new, right_new);
            selects_Join.add(pred);
        }else {
            Attribute left = sl.getPredicate().getLeftAttribute();
            Attribute new_left = new Attribute(left.getName(), left.getValueCount());
            String right_val = sl.getPredicate().getRightValue();
            Predicate pred = new Predicate(new_left, right_val);

            Iterator<Operator> it = scans.iterator();
            Operator now;
            while (it.hasNext()) {
                now = it.next();
                try {
                    if (now.output.getAttributes().contains(sl.getPredicate().getLeftAttribute())) {
                        Select new_sel = new Select(now, pred);
                        new_sel.accept(est);
                        selects_Value.add(new_sel);

                    }
                } catch (ArrayIndexOutOfBoundsException e) {

                }
            }
        }
    }
    public void ScanCoupledWithSelect(){
        Iterator<Operator> it = selects_Value.iterator();
        Operator current;
        while(it.hasNext()){
            current = it.next();
            if(current instanceof Select){
                if(select_scan_coupled.get(((Select) current).getInput())==null){
                    select_scan_coupled.put(((Select) current).getInput(),current);
                }else{
                    Select new_sl = new Select(select_scan_coupled.get(((Select) current).getInput()),((Select) current).getPredicate());
                    new_sl.accept(est);

                    select_scan_coupled.put(((Select) current).getInput(), new_sl);
                }
            }
        }

        Iterator<Operator> it_scan = scans.iterator();
        while(it_scan.hasNext()){
            Operator now = it_scan.next();
            if(select_scan_coupled.get(now) != null){

            }else{
                select_scan_coupled.get(now);
            }
        }
    }

    public void JoinCreation() {
        Attribute aux;
        Attribute aux2;
        Scan new_rel = null;
        Scan new_rel2 = null;
        Operator temp = null;
        Operator temp_r = null;
        for (Predicate sc : selects_Join) {
            aux = sc.getLeftAttribute();
            aux2 = sc.getRightAttribute();
            if (all_scans_att.containsKey(aux)) {
                new_rel = all_scans_att.get(aux);
                temp = new_rel;

                while (select_scan_coupled.containsKey(temp)) {
                    temp = select_scan_coupled.get(temp);
                }
            }
            if (all_scans_att.containsKey(aux2)) {
                new_rel2 = all_scans_att.get(aux2);
                temp_r = new_rel2;
                while (select_scan_coupled.containsKey(temp_r)) {
                    temp_r = select_scan_coupled.get(temp_r);
                }
            }

            if (joins.size() == 0) {
                Join new_j = new Join(temp, temp_r, sc);
                used_scans.add(new_rel);
                used_scans.add(new_rel2);
                joins.add(new_j);
            } else {
                Join new_j = null;
                for (Join j : joins) {
                    System.out.println(j.getPredicate());
                    if (!j.getPredicate().equals(sc)) {
                        if (!used_scans.contains(temp) && !used_scans.contains(temp_r)) {
                            used_scans.add(new_rel);
                            used_scans.add(new_rel2);
                            new_j = new Join(temp, temp_r, sc);
                        } else {
                            if (used_scans.contains(temp) && !used_scans.contains(temp_r)) {
                                used_scans.add(new_rel2);
                                new_j = new Join(j, temp_r, sc);
                            } else {
                                if (!used_scans.contains(temp) && used_scans.contains(temp_r)) {
                                    used_scans.add(new_rel);
                                    new_j = new Join(j, temp, sc);
                                }else{
                                    left_selects.add(new Select(j,(Predicate) sc));
                                }

                            }
                        }
                    }
                }
                if (new_j != null) {
                    if (joins.contains(new_j.getInputs().get(0))) {
                        joins.remove(new_j.getInputs().get(0));
                        joins.add(new_j);
                    } else {


                        joins.add(new_j);
                    }
                }
            }
        }
    }
    public Operator ProductCreation(){


        if(left_selects.size()!=0){
            for(Select j: left_selects){
                if(joins.contains(j.getInput())){
                    op_for_product.add(j);
                }
            }
        }else {
            op_for_product.addAll(joins);
        }
        if(op_for_product.size()!=0) {
            for (Scan sc : all_scans_att.values()) {
                if (!used_scans.contains(sc)) {
                    op_for_product.add(sc);
                    used_scans.add(sc);
                }
            }
        }
        if(op_for_product.size()>1) {
            for (int i = 0; i < op_for_product.size(); i++) {
                if (products.size() == 0) {
                    Product new_prod = new Product(op_for_product.get(i), op_for_product.get(i + 1));
                    products.add(new_prod);
                } else {
                    if(op_for_product.size() > i+1) {
                        System.out.println(products.get(products.size() - 1));
                        Product new_prod = new Product(products.get(products.size() - 1), op_for_product.get(i + 1));
                        products.add(new_prod);
                    }
                }
            }
            return products.get(products.size()-1);
        }else{
            if(op_for_product.size() ==1) {
                return op_for_product.get(0);
            }
        }

        if(selects_Value.size() != 0)
            return selects_Value.poll();

        if(scans.size() ==1) {
            return scans.get(0);
        }else{
            if(scans.size() == 2){
                return new Product(scans.get(0),scans.get(1));
            }else{
                Product new_p = new Product(scans.get(0),scans.get(1));
                scans.remove(0);
                scans.remove(1);
                return new Product(new_p, ProductCreation());
            }
        }

    }
    public Operator MakeProjections(Operator op, List<Attribute> att_needed){
        List<Attribute> now = new ArrayList<>();
       if(op instanceof Product){
           List<Attribute> left_find= new ArrayList<>();
           List<Attribute> right_find= new ArrayList<>();
           List<Attribute> if_left_exist= new ArrayList<>();
           List<Attribute> if_right_exist= new ArrayList<>();
           left_find.addAll(calculateAtt(((Product) op).getLeft()));
           right_find.addAll(calculateAtt(((Product) op).getRight()));

           for(Attribute at:att_needed){
               if(left_find.contains(at)){
                   if_left_exist.add(at);
               }else{
                   if(right_find.contains(at)){
                       if_right_exist.add(at);
                   }
               }
           }

           if(if_left_exist.size()==0){
               if(if_right_exist.size()==0){
                   return op;
               }else{
                   Project PD= new Project(MakeProjections(((Product) op).getRight(),if_right_exist),att_needed);
                   PD.accept(est);
                   Product pr =new Product(((Product) op).getLeft(),PD);
                   pr.accept(est);
                   return pr;
               }
           }else{
               if(if_right_exist.size()==0){
                   Project PL= new Project(MakeProjections(((Product) op).getLeft(),if_left_exist),att_needed);
                   PL.accept(est);
                   Product pr = new Product(PL,((Product) op).getRight());
                   return pr;
               }else{
                   Project PD= new Project(MakeProjections(((Product) op).getRight(),if_right_exist),att_needed);
                   PD.accept(est);
                   Project PL= new Project(MakeProjections(((Product) op).getLeft(),if_left_exist),att_needed);
                   PL.accept(est);
                   Product pr =  new Product(PL,PD);
                   return pr;
               }
           }




       }else {
           if (op instanceof Join) {
               List<Attribute> tempL = new ArrayList<>();
               List<Attribute> tempR = new ArrayList<>();
               List<Attribute> temporary = new ArrayList<>();
               List<Attribute> temporaryR = new ArrayList<>();
               Attribute left = ((Join) op).getPredicate().getLeftAttribute();
               Attribute right =(((Join) op).getPredicate().getRightAttribute());
               Attribute aux;
               att_needed.add(left);
               att_needed.add(right);
               tempL.addAll(calculateAtt(((Join) op).getInputs().get(0)));
               tempR.addAll(calculateAtt(((Join) op).getInputs().get(1)));
               for (Attribute at : att_needed) {
                   if (tempL.contains(at)) {
                       temporary.add(at);
                   } else {
                       if (tempR.contains(at)) {
                           temporaryR.add(at);
                       }
                   }
               }
               if(!tempL.contains(left)){
                   aux= left;
                   left=right;
                   right=aux;
               }
               Project pL = new Project(MakeProjections(((Join) op).getLeft(), att_needed), temporary);
               Project pR = new Project(MakeProjections(((Join) op).getRight(), att_needed), temporaryR);
               pL.accept(est);
               pR.accept(est);
               Join j = new Join(pL, pR, new Predicate(left,right));
               j.accept(est);
               return j;

           }else{
               if(op instanceof Select){
                    if(((Select) op).getPredicate().equalsValue()){
                        return op;
                    }else{
                        List<Attribute> tempL=new ArrayList<>();
                        tempL.addAll(att_needed);
                        tempL.add((((Select) op).getPredicate().getLeftAttribute()));
                        tempL.add((((Select) op).getPredicate().getRightAttribute()));

                        Project pj = new Project( (MakeProjections(((Select) op).getInput(),tempL)),att_needed);
                        pj.accept(est);
                        Select sx = new Select(pj,((Select) op).getPredicate());

                        sx.accept(est);
                        return sx;
                    }
               }else{
                   if(op instanceof Scan){
                       return op;
                   }
               }
           }
       }

       return op; ///schimba
    }

    public List<Attribute> calculateAtt(Operator op){
        List<Attribute> new_att=new ArrayList<>();
        if(op instanceof Product){
            new_att.addAll(calculateAtt(((Product) op).getInputs().get(0)));
            new_att.addAll(calculateAtt(((Product) op).getInputs().get(1)));
            return new_att;
        }

        if(op instanceof Join)
        {
            new_att.add(((Join) op).getPredicate().getLeftAttribute());
            new_att.add(((Join) op).getPredicate().getRightAttribute());
            new_att.addAll(calculateAtt(((Join) op).getInputs().get(0)));
            new_att.addAll(calculateAtt(((Join) op).getInputs().get(1)));
            return new_att;
        }

        if(op instanceof Select) {
            if (((Select) op).getPredicate().equalsValue()) {
                new_att.add(((Select)op).getPredicate().getLeftAttribute());
                new_att.addAll(calculateAtt(((Select) op).getInput()));
                return new_att;
            }else{
                new_att.add(((Select)op).getPredicate().getLeftAttribute());
                new_att.add(((Select)op).getPredicate().getRightAttribute());
                new_att.addAll(calculateAtt(((Select) op).getInput()));
                return new_att;
            }
        }
        if(op instanceof Scan) {
            new_att.addAll(((Scan) op).getRelation().getAttributes());
            return new_att;
        }
        return null;
    }


}
